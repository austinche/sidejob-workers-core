# A workflow graph specifies data flow between jobs and ports
# Runs a graph in the noflo json format 

# Don't register this worker
# It is queued and handled specially by the UI

module Workers
  class Workflow
    include SideJob::Worker

    # Loads graph from elasticsearch and runs a graph
    # @param workflow_id [String] Workflow ID of graph to run
    # @param graph [String] noflo graph in JSON format https://github.com/noflo/noflo/blob/master/graph-schema.json
    def perform(graph)
      graph = JSON.parse(graph)

      jids = get_json(:jobs) || {} # store graph process name -> job ids

      @jobs = {} # graph process name -> SideJob::Job
      new_jobs = Set.new # jobs that were started in this run

      # make sure all jobs are started
      graph['processes'].each_pair do |name, info|
        @jobs[name] = SideJob.find(jids[name]) if jids[name]
        if ! @jobs[name]
          # start a new job
          # component name must be of form queue/ClassName
          # workflow/<id> is a special case for recursively running a workflow
          queue, klass = info['component'].split('/', 2)
          raise "Unable to parse #{info['component']}: Must be of form queue/ClassName" if ! queue || ! klass

          if queue == 'workflow'
            workflow = Ginkgo.elasticsearch.get(index: 'workflows', type: 'workflow', id: klass)
            raise "Unable to find Workflow/#{klass}" unless workflow['found']
            job = queue('core', 'Workers::Workflow', args: [workflow['_source']['graph']])
          else
            job = queue(queue, klass)
          end

          jids[name] = job.jid
          set_json :jobs, jids

          @jobs[name] = job
          new_jobs << name
        end
      end

      connections = {} # SideJob::Port (output port) -> Array<SideJob::Port> (input ports)
      graph['connections'].each do |connection|
        tgt_port = get_port(:in, connection['tgt'])

        if connection['data']
          # initial fixed data to be sent only once
          if new_jobs.include?(connection['tgt']['process'])
            tgt_port.write connection['data']
          end
        else
          src_port = get_port(:out, connection['src'])

          connections[src_port] ||= []
          connections[src_port] << tgt_port
        end
      end

      # outport connections have to be merged with job connections in case
      # some data needs to go to both another job and a graph outport
      if graph['outports']
        graph['outports'].each_pair do |name, port|
          out = get_port(:out, port)
          connections[out] ||= []
          connections[out] << output(name)
        end
      end

      # process all connections

      if graph['inports']
        graph['inports'].each_pair do |name, port|
          connect_ports(input(name), [get_port(:in, port)])
        end
      end

      connections.each_pair do |port, targets|
        connect_ports(port, targets)
      end

      # we complete if all jobs are completed
      # if any job is failed, we fail also
      @jobs.each_pair do |name, job|
        case job.status
        when :completed
        when :failed
          raise "#{name}: #{job.get(:error)}"
        else
          suspend
          return
        end
      end
    end

    # @param source SideJob::Port
    # @param targets [Array<SideJob::Port>]
    def connect_ports(source, targets)
      # copy the output to multiple ports
      loop do
        data = source.read
        break unless data
        targets.each do |target|
          port = get_port(:in, target)
          port.write data
        end
      end
    end

    # @param type [:in, :out]
    # @param data [Hash, Port] {'process' => '...', 'port' => '...'}. If Port given, just returns it
    # @return [SideJob::Port]
    def get_port(type, data)
      return data if data.is_a?(SideJob::Port)
      job = @jobs[data['process']]
      if type == :in
        job.input(data['port'])
      elsif type == :out
        job.output(data['port'])
      else
        raise "Invalid port type: #{type}"
      end
    end
  end
end
