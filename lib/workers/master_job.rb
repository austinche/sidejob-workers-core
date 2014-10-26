module Workers
  class MasterJob
    include SideJob::Worker
    register(
        description: 'Queues and writes/reads data from child jobs',
        icon: 'sitemap',
        inports: {
            inport: { type: 'object', description: 'Write some data to the inport of a child job: name, port, and data are required' },
            queue: { type: 'object', description: 'Options for new job: at least queue, class, and name are required' },
        },
        outports: {
            outport: { type: 'object', description: 'Data on child outports are sent here' },
        },
    )

    def perform
      for_inputs(:queue) do |options|
        queue = options.delete('queue')
        klass = options.delete('class')
        queue(queue, klass, **options.symbolize_keys)
      end

      for_inputs(:inport) do |data|
        name = data['name'] or raise 'Missing required name key for inport data specification'
        port = data['port'] or raise 'Missing required port key for inport data specification'
        data = data['data']
        job = child(name) or raise "Unable to find job named #{name}"
        job.input(port).write data
      end

      out = output(:outport)
      children.each_pair do |name, job|
        job.outports.each do |outport|
          outport.each do |data|
            out.write({name: name, id: job.id, port: outport.name, data: data})
          end
        end
      end
    end
  end
end
