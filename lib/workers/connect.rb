module Workers
  class Connect
    include SideJob::Worker
    register(
        description: 'Connects the input to output port',
        icon: 'arrow-right',
        inports: {
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Output data' },
        },
    )

    def perform
      input(:in).connect_to output(:out)
    end
  end
end
