module Workers
  class Drop
    include SideJob::Worker
    register(
        description: 'Drops all data',
        icon: 'trash-o',
        inports: {
            in: { type: 'all', description: 'Input data' },
        },
        outports: {},
    )

    def perform
      input(:in).entries
    end
  end
end
