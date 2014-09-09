module Workers
  class Placeholder
    include SideJob::Worker
    register('core', 'Workers::Placeholder', {
        description: 'Does nothing',
        icon: 'ellipsis-h',
        inports: [
            { name: 'in', type: 'all', description: 'Input (ignored)' },
        ],
        outports: [
            { name: 'out', type: 'all', description: 'Output (ignored)' },
        ],
    })

    def perform
    end
  end
end
