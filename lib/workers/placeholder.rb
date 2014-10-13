module Workers
  class Placeholder
    include SideJob::Worker
    register(
        description: 'Does nothing',
        icon: 'ellipsis-h',
        inports: {
            '*' => { type: 'all', description: 'Input (ignored)' },
        },
        outports: {
            '*' => { type: 'all', description: 'Output (ignored)' },
        },
    )

    def perform
    end
  end
end
