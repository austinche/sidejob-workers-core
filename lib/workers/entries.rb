module Workers
  class Entries
    include SideJob::Worker
    register(
        description: 'Outputs the individual entries from an array or object.',
        icon: 'list',
        inports: {
            in: { type: 'all', description: 'Input object or array' },
        },
        outports: {
            out: { type: 'all', description: 'Elements of array or [key, value] from object' },
        },
    )

    def perform
      for_inputs(:in) do |input|
        input.each do |x|
          output(:out).write x
        end
      end
    end
  end
end
