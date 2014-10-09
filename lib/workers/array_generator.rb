module Workers
  class ArrayGenerator
    include SideJob::Worker
    register('core', 'Workers::ArrayGenerator', {
        description: 'Generates JSON arrays by concatenating multiple inputs',
        icon: 'list-ol',
        inports: [
            { name: 'config', type: 'array', description: 'Array of objects specifying the arrays to be concatenated in order. Each object has keys: port (required), use_recent, collect, from_object' },
            { name: 'in1', type: 'all' },
            { name: 'in2', type: 'all' },
            { name: 'in3', type: 'all' },
            { name: 'in4', type: 'all' },
        ],
        outports: [
            { name: 'out', type: 'array', description: 'Output arrays' },
        ],
    })

    def perform
      config = get_config(:config)
      suspend unless config

      current = get_config(:current) || {} # port -> current object data
      loop do
        any = false
        all = true

        # load in current values
        config.each do |item|
          port = item['port']
          raise 'Missing port in config' unless port

          if item['collect']
            # collect a certain number of elements on the port and make an array
            raise 'use_recent option ignored with collect' if item['use_recent']
            current[port] ||= []
            loop do
              if current[port].length >= item['collect']
                any = true
                break
              else
                break if input(port).size == 0
                data = input(port).read
                current[port] << array_from_input(data, item)
                any = true
              end
            end
            all = false if current[port].length < item['collect']
          else
            if item['use_recent']
              # only the most recent data is used and it is saved for reuse
              data = array_from_input(input(port).drain.last, item)
              current[port] = data if ! data.nil?
            else
              # default is to read data one at a time
              if current[port].nil?
                current[port] = array_from_input(input(port).read, item)
              end
              any = true if current[port]
            end

            all = false unless current[port]
          end
        end

        set({current: current})

        # complete if no inputs, suspend if partial inputs
        return unless any
        suspend unless all

        # now concatenate all data and generate output
        result = config.each_with_object([]) do |item, result|
          port = item['port']
          result.concat(current[port])
          current.delete(port) unless item['use_recent']
        end
        output(:out).write result

        set({current: current})
      end
    end

    private

    def array_from_input(data, item)
      return nil if data.nil?
      data = data.to_a if item['from_object']
      data
    end
  end
end
