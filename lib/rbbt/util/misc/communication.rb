module Misc
  PUSHBULLET_KEY=begin
                   if ENV["PUSHBULLET_KEY"]
                     ENV["PUSHBULLET_KEY"]
                   else
                     config_api = File.join(ENV['HOME'], 'config/apps/pushbullet/apikey')
                     if File.exist? config_api
                       File.read(config_api).strip
                     else
                       nil
                     end
                   end
                 end

  def self.notify(description, event='notification', key = nil)
    if PUSHBULLET_KEY.nil? and key.nil?
      Log.warn "Could not notify, no PUSHBULLET_KEY"
      return
    end

    Thread.new do
      application = 'rbbt'
      event ||= 'notification'
      key ||= PUSHBULLET_KEY
      `curl -s --header "Authorization: Bearer #{key}" -X POST https://api.pushbullet.com/v2/pushes --header 'Content-Type: application/json' --data-binary '{"type": "note", "title": "#{event}", "body": "#{description}"}'`
    end
  end

  def self.send_email_old(from, to, subject, message, options = {})
    IndiferentHash.setup(options)
    options = Misc.add_defaults options, :from_alias => nil, :to_alias => nil, :server => 'localhost', :port => 25, :user => nil, :pass => nil, :auth => :login

    server, port, user, pass, from_alias, to_alias, auth = Misc.process_options options, :server, :port, :user, :pass, :from_alias, :to_alias, :auth

    msg = <<-END_OF_MESSAGE
From: #{from_alias} <#{from}>
To: #{to_alias} <#{to}>
Subject: #{subject}

#{message}
END_OF_MESSAGE

    Net::SMTP.start(server, port, server, user, pass, auth) do |smtp|
      smtp.send_message msg, from, to
    end
  end

  def self.send_email(from, to, subject, message, options = {})
    require 'mail'

    IndiferentHash.setup(options)
    options = Misc.add_defaults options, :from_alias => nil, :to_alias => nil, :server => 'localhost', :port => 25, :user => nil, :pass => nil, :auth => :login, :files => []

    server, port, user, pass, from_alias, to_alias, auth, files = Misc.process_options options, :server, :port, :user, :pass, :from_alias, :to_alias, :auth, :files

    files = [] if files.nil?
    files = [files] unless Array === files

    Mail.defaults do
      delivery_method :smtp, address: server, port: port, user_name: user, password: pass
    end

    mail = Mail.deliver do 
      from  "#{from_alias} <#{from}>"
      to "#{to_alias} <#{to}>"
      subject subject

      text_part do 
        body message
      end

      files.each do |file|
        file = file.find if Path === file
        file = file.path if Step === file
      end
    end
  end

end
