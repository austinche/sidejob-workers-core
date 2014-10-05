module Workers
  class MasterJob
    include SideJob::Worker
    register('core', 'Workers::MasterJob', {
        description: 'Queues and writes/reads data from child jobs',
        icon: 'sitemap',
        inports: [
            { name: 'inport', type: 'object', description: 'Write some data to the inport of a child job' },
            { name: 'queue', type: 'object', description: 'Options for new job: at least queue and class are required' },
        ],
        outports: [
            { name: 'outport', type: 'object', description: 'Data on child outports are sent here' },
        ],
    })

    def perform
      names = get(:names) || {} # mapping from job names to job ids
      input(:queue).each do |options|
        name = options.delete('name')
        queue = options.delete('queue')
        klass = options.delete('class')
        child = queue(queue, klass, **options.symbolize_keys)
        if name
          names[name.to_s] = child.jid
          set(names: names)
        end
      end

      input(:inport).each do |data|
        name = data['name'] or raise 'Missing required name key for inport data specification'
        port = data['port'] or raise 'Missing required port key for inport data specification'
        data = data['data']
        child = SideJob.find(names[name]) or raise "Unable to find job named #{name}"
        child.input(port).write data
      end

      out = output(:outport)
      children.each do |child|
        name = names.key(child.jid)
        child.outports.each do |outport|
          outport.each do |data|
            out.write({name: name, id: child.jid, port: outport.name, data: data})
          end
        end
      end
    end
  end
end
