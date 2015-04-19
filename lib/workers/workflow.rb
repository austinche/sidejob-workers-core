# A workflow graph specifies data flow between jobs and ports

module Workers
  class Workflow
    include SideJob::Worker
    register(
        inports: {
            '__graph' => { type: 'object', description: 'Set workflow graph' },
            '*' => { type: 'all', description: 'Workflow inport' },
        },
        outports: {
            '*' => { type: 'all', description: 'Workflow outport' },
        },
    )

    # Workflow graphs should either be sent in via the __graph port or
    # stored in SideJob.redis under the workflows key with the given workflow_id.
    # See the spec for examples of the graph format.
    def perform(workflow_id=nil)
      @nodes = children # graph node id -> SideJob::Job

      # group so graph read and init data are together
      SideJob::Port.log_group do
        new_graph = input('__graph').entries.last
        if workflow_id && ! new_graph && ! get(:graph)
          new_graph = JSON.parse(SideJob.redis.hget('workflows', workflow_id)) rescue nil
        end

        if new_graph
          @graph = new_graph
          set({graph: @graph})
          init_graph
        else
          @graph = get(:graph)
          suspend unless @graph
        end
      end

      @graph['nodes'] ||= {}
      @graph['edges'] ||= []
      @graph['inports'] ||= {}
      @graph['outports'] ||= {}

      # disown any jobs that are no longer in the graph
      reload = false
      @nodes.each_key do |node|
        if ! @graph['nodes'][node]
          disown(node)
          reload = true
        end
      end
      @nodes = children if reload

      connections = {} # SideJob::Port (output port) -> Array<SideJob::Port>
      @graph['edges'].each do |connection|
        src_job = @nodes[connection['from']['node']]
        next unless src_job      # No data possible if the node has not been started
        src_port = src_job.output(connection['from']['outport'])
        next unless src_port.size > 0
        tgt_job = ensure_started(connection['to']['node'])
        connections[src_port] ||= []
        connections[src_port] << tgt_job.input(connection['to']['inport'])
      end

      # outport connections have to be merged with job connections in case
      # some data needs to go to both another job and a graph outport
      @graph['outports'].each_pair do |name, port|
        job = @nodes[port['node']]
        next unless job
        out = job.output(port['outport'])
        connections[out] ||= []
        connections[out] << output(name)
      end

      # process all connections

      @graph['inports'].each_pair do |name, port|
        inport = input(name)
        if inport.size > 0
          job = ensure_started(port['node'])
          inport.connect_to job.input(port['inport'])
        end
      end

      connections.each_pair do |port, targets|
        port.connect_to targets
      end

      # we complete if all jobs are completed
      # if any job is failed, we fail also
      @nodes.each_pair do |name, job|
        case job.status
          when 'completed'
          when 'failed'
            raise "Job #{job.id} failed"
          else
            suspend
        end
      end
    end

    private

    # Returns and starts if necessary the job associated with a graph node
    # @param node [String] node ID from graph
    # @return [SideJob::Job] job for the given node
    def ensure_started(node)
      return @nodes[node] if @nodes[node]

      info = @graph['nodes'][node]
      raise "Cannot find node #{node} in graph" unless info

      queue = info['queue']
      klass = info['class']
      raise "Missing required queue or class metadata for node #{node}" if ! queue || ! klass

      job = queue(queue, klass, name: node, args: info['args'],
                  inports: info['inports'], outports: info['outports'])
      @nodes[node] = job
      return job
    end

    def init_ports(job, data)
      job.inports = data['inports']
      job.outports = data['outports']
    end

    # Initialize graph
    # Run once on each new graph
    def init_graph
      SideJob::Port.log_group do
        (@graph['nodes'] || {}).each_pair do |node, data|
          job = @nodes[node]
          init_ports(job, data) if job

          if data['init'].is_a?(Integer)
            raise "Job #{data['init']} cannot be adopted because node #{node} has been started as job #{job.id}" if job
            job = SideJob.find(data['init'])
            raise "Job #{data['init']} does not exist" unless job
            if job.get(:queue) == data['queue'] && job.get(:class) == data['class'] && job.get(:args) == data['args']
              adopt(job, node)
            else
              raise "Job #{data['init']} cannot be adopted due to param mismatch with node #{node}"
            end
            init_ports(job, data)
          elsif data['init']
            job = ensure_started(node)
          end

          %i{in out}.each do |type|
            (data["#{type}ports"] || {}).each_pair do |name, options|
              if options['init']
                job = ensure_started(node) if ! job
                port = job.send("#{type}put", name)
                options['init'].each do |data|
                  port.write(data)
                end
              end
            end
          end
        end
      end
    end
  end
end
