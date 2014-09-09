module Workers
  class Drop
    include SideJob::Worker
    register('core', 'Workers::Drop', {
        description: 'Drops all data',
        icon: 'trash-o',
        inports: [
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
        ],
    })

    def perform
      input(:in).drain
    end
  end
end
