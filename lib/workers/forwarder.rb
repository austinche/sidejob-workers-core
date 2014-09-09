module Workers
  class Forwarder
    include SideJob::Worker
    register('core', 'Workers::Forwarder', {
        description: 'Forwards all data on input port to output port',
        icon: 'forward',
        inports: [
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'all', description: 'Output data' },
        ],
    })

    def perform
      output(:out).write *input(:in).drain
    end
  end
end
