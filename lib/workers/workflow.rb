require 'ginkgo/elasticsearch'

# A workflow graph specifies data flow between jobs and ports
# Runs a graph in the noflo json format with some additions to metadata

module Workers
  class Workflow
    include SideJob::Worker
    register(
        inports: {
            '*' => { type: 'all', description: 'Workflow inport' },
        },
        outports: {
            '*' => { type: 'all', description: 'Workflow outport' },
        },
    )

    # Loads graph from elasticsearch and runs a workflow
    # The graph is expected to be in noflo graph JSON format https://github.com/noflo/noflo/blob/master/graph-schema.json
    def perform(workflow_id)
      # use job specific graph if it exists, otherwise load in graph from workflow
      es_job = Ginkgo.ES.get_source(index: 'jobs', type: 'job', id: id, ignore: 404)
      if es_job && es_job['graph']
        @graph = JSON.parse(es_job['graph'])
      else
        workflow = Ginkgo.ES.get_source(index: 'workflows', type: 'workflow', id: workflow_id, ignore: 404)
        raise "Unable to find Workflow/#{workflow_id}" unless workflow && workflow['graph']
        @graph = JSON.parse(workflow['graph'])
        es_update_or_create id, {
            queue: get(:queue), class: get(:class), args: get(:args), # include these fields for lims/ui on first load
            graph: @graph.to_json,
        }
      end

      @nodes = children # graph node id -> SideJob::Job

      (@graph['processes'] || {}).each_key do |node|
        node_job(node)
      end

      connections = {} # SideJob::Port (output port) -> Array<Hash|SideJob::Port> {'process' => '...', 'port' => '...'}
      (@graph['connections'] || []).each do |connection|
        src_job = @nodes[connection['src']['process']]
        next unless src_job      # No data possible if the node has not been started
        src_port = src_job.output(connection['src']['port'])

        tgt_job = node_job(connection['tgt']['process'], force_start: true)

        connections[src_port] ||= []
        connections[src_port] << tgt_job.input(connection['tgt']['port'])
      end

      # outport connections have to be merged with job connections in case
      # some data needs to go to both another job and a graph outport
      if @graph['outports']
        @graph['outports'].each_pair do |name, port|
          job = @nodes[port['process']]
          next unless job
          out = job.output(port['port'])
          connections[out] ||= []
          connections[out] << output(name)
        end
      end

      # process all connections

      if @graph['inports']
        @graph['inports'].each_pair do |name, port|
          job = node_job(port['process'], force_start: true)
          input(name).connect_to job.input(port['port']), job: id
        end
      end

      connections.each_pair do |port, targets|
        port.connect_to targets, job: id
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

    # lims/ui and lims/indexer may also be updating the job, so we need to be careful to not overwrite
    # data until we have upsert (elasticsearch 1.4).
    # try update first and if that fails then try create
    def es_update_or_create(job_id, doc)
      Ginkgo.ES.update(index: 'jobs', type: 'job', id: job_id, ignore: 404, body: { doc: doc }) ||
          Ginkgo.ES.create(index: 'jobs', type: 'job', id: job_id, body: doc)
    end

    # Returns the job associated with a graph node
    # @param node [String] node ID from graph
    # @param force_start [Boolean] If true, will always start a job if no job has been started
    # @return [SideJob::Job, nil] job for the given node or nil if one hasn't been started
    def node_job(node, force_start: false)
      return @nodes[node] if @nodes[node]

      info = @graph['processes'][node]
      raise "Cannot find node #{node} in graph" unless info

      queue = info['metadata']['queue']
      klass = info['metadata']['class']
      raise "Missing required queue or class metadata for node #{node}" if ! queue || ! klass

      # we only start nodes that have initial data defined (can be empty) or that have received input data
      init = (info['metadata']['inports'] || {}).values.any? {|port| port['data']} ||
          (info['metadata']['outports'] || {}).values.any? {|port| port['data']}

      return nil unless init || force_start

      job = queue(queue, klass, name: node, args: info['metadata']['args'],
                  inports: info['metadata']['inports'], outports: info['metadata']['outports'])
      @nodes[node] = job

      job
    end
  end
end
