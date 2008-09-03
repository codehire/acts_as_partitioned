namespace :partitions do
  task :load_classes => :environment do
    Dir.new(File.join("#{RAILS_ROOT}", 'app', 'models')).each do |f|
      if File.extname(f) == '.rb'
        begin
          require File.basename(f, '.rb')
        rescue
        end
      end
    end
    # Load plugins
    Dir["#{RAILS_ROOT}/vendor/plugins/**"].each do |dir|
      if File.directory?("#{dir}/lib")
        Dir.new("#{dir}/lib").each do |f|
          puts f
          if File.extname(f) == '.rb'
            begin
              require File.basename(f, '.rb')
            rescue
            end
          end
        end
      end
    end
  end

  desc "Create future partitions"
  task :create_future => :load_classes do
    puts "Creating future parts."
    ActiveRecord::Base.partitioned_classes.each do |klass|
      # TODO: For now assume that all models are partitioned on date
      # Take today's date and tomorrow
      date = Date.today
      begin
        klass.create_partition(date)
        klass.create_partition(date + 1)
      rescue
      end
      # Deactivate old parts and activate new parts
      currents, olds = klass.find_partition(:all).partition { |i| i.partition_date >= Date.today }
      currents.each(&:activate!)
      olds.each(&:deactivate!)
      (currents + olds).each { |p| p.grant_to :cortex }
    end
  end

  # TODO: Config option for where dumps go??
  desc "Delete and possibly archive old data"
  task :archive => :load_classes do
    ActiveRecord::Base.partitioned_classes.each do |klass|
      puts "#{klass}: Running task for partitioned tables older than #{klass.partition_dump_age} days"
      if klass.partition_dump_age > 0
        "Running task for partitioned tables older than #{klass.partition_dump_age}"
        old = klass.find_partition(:all).select do |part|
          part.partition_date < (Date.today - klass.partition_dump_age)
        end
        if klass.partition_archive?
          old.each do |part|
            puts "Archiving"
            File.open("/data/archive/#{part.tablename}.sql.gz", "w+") do |file|
              file.write part.dump
            end
          end
        end
        old.each(&:drop!)
      end
    end
  end

  desc "Analyze today's partitions"
  task :analyze => :load_classes do
    ActiveRecord::Base.partitioned_classes.each do |klass|
      puts "Analyzing latest tables for model #{klass}"
      klass.find_partition(Date.today).analyze!
    end
  end
end
