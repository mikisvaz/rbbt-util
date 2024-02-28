require 'rbbt/util/R'

module Workflow
  def self.trace_job_times(jobs, fix_gap = false)
    data = TSV.setup({}, "Job~Code,Workflow,Task,Start,End#:type=:list")
    min_start = nil
    max_done = nil
    jobs.each do |job|
      next unless job.info[:done]
      started = job.info[:started]
      ddone = job.info[:done]

      started = Time.parse started if String === started
      ddone = Time.parse ddone if String === ddone

      code = [job.workflow, job.task_name].compact.collect{|s| s.to_s} * " · "
      code = job.name + " - " + code

      data[job.path] = [code,job.workflow.to_s, job.task_name, started, ddone]
      if min_start.nil?
        min_start = started
      else
        min_start = started if started < min_start
      end

      if max_done.nil?
        max_done = ddone
      else
        max_done = ddone if ddone > max_done
      end
    end

    data.add_field "Start.second" do |k,value|
      value["Start"] - min_start
    end

    data.add_field "End.second" do |k,value|
      value["End"] - min_start
    end

    if fix_gap
      ranges = []
      data.through do |k,values|
        start, eend = values.values_at "Start.second", "End.second"

        ranges << (start..eend)
      end

      gaps = {}
      last = nil
      Misc.collapse_ranges(ranges).each do |range|
        start = range.begin
        eend = range.end
        if last
          gaps[last] = start - last
        end
        last = eend
      end

      data.process "End.second" do |value,k,values|
        gap = Misc.sum(gaps.select{|pos,size| pos < values["Start.second"]}.collect{|pos,size| size})
        value - gap
      end

      data.process "Start.second" do |value,k,values|
        gap = Misc.sum(gaps.select{|pos,size| pos < values["Start.second"]}.collect{|pos,size| size})
        value - gap
      end

      total_gaps = Misc.sum(gaps.collect{|k,v| v})
      Log.info "Total gaps: #{total_gaps} seconds"
    end

    start = data.column("Start.second").values.flatten.collect{|v| v.to_f}.min
    eend = data.column("End.second").values.flatten.collect{|v| v.to_f}.max
    total = eend - start unless eend.nil? || start.nil?
    Log.info "Total time elapsed: #{total} seconds" if total

    data
  end

  def self.plot_trace_job_times(data, plot, width=800, height=800)
    data.R <<-EOF, [:svg]
rbbt.require('dplyr')
rbbt.require('tidyr')
rbbt.require('ggplot2')

names(data) <- make.names(names(data))
data$id = data$Code
data$content = data$Task
data$start = data$Start
data$end = data$End
data$Project = data$Workflow

tasks = data

#theme_gantt <- function(base_size=11, base_family="Source Sans Pro Light") {
theme_gantt <- function(base_size=11, base_family="Sans Serif") {
  ret <- theme_bw(base_size, base_family) %+replace%
    theme(panel.background = element_rect(fill="#ffffff", colour=NA),
          axis.title.x=element_text(vjust=-0.2), axis.title.y=element_text(vjust=1.5),
          title=element_text(vjust=1.2, family="Source Sans Pro Semibold"),
          panel.border = element_blank(), axis.line=element_blank(),
          panel.grid.minor=element_blank(),
          panel.grid.major.y = element_blank(),
          panel.grid.major.x = element_line(size=0.5, colour="grey80"),
          axis.ticks=element_blank(),
          legend.position="bottom", 
          axis.title=element_text(size=rel(1.2), family="Source Sans Pro Semibold"),
          strip.text=element_text(size=rel(1.5), family="Source Sans Pro Semibold"),
          strip.background=element_rect(fill="#ffffff", colour=NA),
          panel.spacing.y=unit(1.5, "lines"),
          legend.key = element_blank())

  ret
}

tasks.long <- tasks %>%
gather(date.type, task.date, -c(Code,Project, Task, id, Start.second, End.second)) %>%
arrange(date.type, task.date) %>%
mutate(id = factor(id, levels=rev(unique(id)), ordered=TRUE))

x.breaks <- seq(length(tasks$Task) + 0.5 - 3, 0, by=-3)

timeline <- ggplot(tasks.long, aes(y=id, yend=id, x=Start.second, xend=End.second, colour=Task)) + 
  geom_segment() + 
  geom_vline(xintercept=x.breaks, colour="grey80", linetype="dotted") + 
  guides(colour=guide_legend(title=NULL)) +
  labs(x=NULL, y=NULL) + 
  theme_gantt() + theme(axis.text.x=element_text(angle=45, hjust=1))

rbbt.png_plot('#{plot}', 'plot(timeline)', width=#{width}, height=#{height}, pointsize=6)
    EOF
  end

  def self.trace_job_summary(jobs, report_keys = [])
    tasks_info = {}

    report_keys = report_keys.collect{|k| k.to_s}

    jobs.each do |dep|
      next unless dep.info[:done]
      task = [dep.workflow, dep.task_name].compact.collect{|s| s.to_s} * "#"
      info = tasks_info[task] ||= IndiferentHash.setup({})
      dep_info = IndiferentHash.setup(dep.info)

      ddone = dep_info[:done]
      started = dep_info[:started]

      started = Time.parse started if String === started
      ddone = Time.parse ddone if String === ddone

      time = ddone - started
      info[:time] ||= []
      info[:time] << time

      report_keys.each do |key|
        info[key] = dep_info[key] 
      end

      dep.info[:config_keys].each do |kinfo| 
        key, value, tokens = kinfo

        info[key.to_s] = value if report_keys.include? key.to_s
      end if dep.info[:config_keys]
    end

    summary = TSV.setup({}, "Task~Calls,Avg. Time,Total Time#:type=:list")

    tasks_info.each do |task, info|
      time_lists = info[:time]
      avg_time = Misc.mean(time_lists).to_i
      total_time = Misc.sum(time_lists).to_i
      calls = time_lists.length
      summary[task] = [calls, avg_time, total_time]
    end

    report_keys.each do |key|
      summary.add_field Misc.humanize(key) do |task|
        tasks_info[task][key]
      end
    end if Array === report_keys && report_keys.any?

    summary
  end

  def self.trace(seed_jobs, options = {})
    jobs = []
    seed_jobs.each do |step|
      jobs += step.rec_dependencies + [step]
      step.info[:archived_info].each do |path,ainfo|
        next unless Hash === ainfo
        archived_step = Step.new path

        archived_step.define_singleton_method :info do
          ainfo
        end

        #class << archived_step
        #  self
        #end.define_method :info do
        #  ainfo
        #end

        jobs << archived_step
      end if step.info[:archived_info]

    end

    jobs = jobs.uniq.sort_by{|job| [job, job.info]; t = job.info[:started] || Open.mtime(job.path) || Time.now; Time === t ? t : Time.parse(t) }

    data = trace_job_times(jobs, options[:fix_gap])

    report_keys = options[:report_keys] || ""
    report_keys = report_keys.split(/,\s*/) if String === report_keys
    summary = trace_job_summary(jobs, report_keys)

    raise "No jobs to process" if data.size == 0

    plot, size, width, height = options.values_at :plot, :size, :width, :height

    size = 800 if size.nil?
    width = size.to_i * 2 if width.nil?
    height = size  if height.nil?

    plot_trace_job_times(data, plot, width, height) if plot

    if options[:plot_data]
      data
    else
      summary
    end

  end
end
