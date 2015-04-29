# Map is designed to work with a second job
# An input array or object enters on input(:in) and each entry sent to output(:each)
# From there, the elements are sent to the second job
# The output value for each element should be fed back into input(:each)
# Then Map outputs the mapped array to output(:out)
module Workers
  class Map
    include SideJob::Worker
    register(
        description: 'Maps elements in an array or object',
        icon: 'map-marker',
        inports: {
            in: { type: 'all', description: 'Input array or object' },
            each: { type: 'all', description: 'Each mapped value' },
        },
        outports: {
            each: { type: 'all', description: 'Elements of array or [key, value] from object' },
            out: { type: 'array', description: 'Output array with mapped values' },
        },
    )

    def perform
      loop do
        n = get(:n)
        if ! n
          raise 'Cannot handle default on input port' if input(:in).default?
          return unless input(:in).data?
          source = input(:in).read
          raise 'Input must be an array or object' unless source.respond_to?(:each)
          n = source.length
          set({n: n})
          source.each {|x| output(:each).write x}
        end

        values = get(:values) || []

        while n > values.length && input(:each).data?
          values << input(:each).read
        end

        if n == values.length
          # got all mapped values, write out
          output(:out).write values
          unset('n', 'values')
        else
          set({values: values})
          suspend
        end
      end
    end
  end
end
