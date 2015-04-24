module Workers
  class GroupsOf
    include SideJob::Worker
    register(
        description: 'Splits an array into groups of a certain size',
        icon: 'reorder',
        inports: {
            in: { type: 'array', description: 'Input array' },
            size: { type: 'integer', description: 'Size of each array group (except possibly the last one)' },
            fill: { default: false, type: 'all', description: 'If non-false, pads any extra spots in the last group with the fill value' },
        },
        outports: {
            out: { type: 'array', description: 'Output array of group arrays' },
        },
    )

    def perform
      for_inputs(:in, :size, :fill) do |input, size, fill|
        # based on activesupport Array#in_groups_of
        raise 'Size must be a positive number' if size.to_i <= 0
        if fill == false
          collection = input
        else
          padding = (size - input.size % size) % size
          collection = input.concat(::Array.new(padding, fill))
        end
        output(:out).write collection.each_slice(size).to_a
      end
    end
  end
end
