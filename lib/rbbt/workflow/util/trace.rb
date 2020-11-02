require 'rbbt/util/R'

module Workflow
  def self.trace(seed_jobs, options = {})

    jobs = []
    seed_jobs.each{|j| jobs << j; jobs += j.rec_dependencies}

    data = TSV.setup({}, "Job~Workflow,Task,Start,End#:type=:list")
    min_start = nil
    max_done = nil
    jobs.each do |job|
      next unless job.info[:done]
      started = job.info[:started]
      ddone = job.info[:done]

      code = [job.workflow, job.task_name].compact.collect{|s| s.to_s} * "."
      code = code + '.' + job.name

      data[code] = [job.workflow.to_s, job.task_name, started, ddone]
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

    if options[:fix_gap]
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
    end

    tasks_info = {}

    jobs.each do |dep|
      next unless dep.info[:done]
      task = [dep.workflow, dep.task_name].compact.collect{|s| s.to_s} * "#"
      info = tasks_info[task] ||= {}

      time = dep.info[:done] - dep.info[:started]
      info[:time] ||= []
      info[:time] << time

      cpus = nil
      spark = false
      shard = false
      dep.info[:config_keys].select do |kinfo| 
        key, value, tokens = kinfo
        key = key.to_s
        cpus = value if key.include? 'cpu'
        spark = value if key == 'spark'
        shard = value if key == 'shard'
      end

      info[:cpus] = cpus || 1
      info[:spark] = spark
      info[:shard] = shard
    end

    stats = TSV.setup({}, "Task~Calls,Avg. Time,Total Time,Cpus,Spark,Shard#:type=:list")

    tasks_info.each do |task, info|
      time_lists, cpus, spark, shard = info.values_at :time, :cpus, :spark, :shard
      avg_time = Misc.mean(time_lists)
      total_time = Misc.sum(time_lists)
      calls = time_lists.length
      stats[task] = [calls, avg_time, total_time, cpus, spark, shard]
    end

    raise "No jobs to process" if data.size == 0

    start = data.column("Start.second").values.flatten.collect{|v| v.to_f}.min
    eend = data.column("End.second").values.flatten.collect{|v| v.to_f}.max
    total = eend - start
    Log.info "Total time elapsed: #{total} seconds"

    if options[:fix_gap]
      total_gaps = Misc.sum(gaps.collect{|k,v| v})
      Log.info "Total gaps: #{total_gaps} seconds"
    end

    plot, width, height = options.values_at :plot, :width, :height
    if plot
      data.R <<-EOF, [:svg]
    rbbt.require('tidyverse')
    rbbt.require('ggplot2')

    names(data) <- make.names(names(data))
    data$id = rownames(data)
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
    gather(date.type, task.date, -c(Project, Task, id, Start.second, End.second)) %>%
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

    if options[:plot_data]
      data
    else
      stats
    end

  end
end
