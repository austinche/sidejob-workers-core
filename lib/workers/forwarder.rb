module Workers
  class Forwarder
    include SideJob::Worker
    register(
        description: 'Forwards all data on input port to output port',
        icon: 'forward',
        inports: {
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Output data' },
        },
    )

    def perform
      for_inputs(:in) do |data|
        output(:out).write data
      end
    end
  end
end
