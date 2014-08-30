# expects jq to be in the path

module Workers
  class Filter
    include SideJob::Worker
    register('core', 'Workers::Filter', {
        description: 'Runs a jq filter',
        icon: 'filter',
        inports: [
            { name: 'filter', type: 'string', description: 'Filter in the jq language: http://stedolan.github.io/jq/' },
            { name: 'in', type: 'string', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'string', description: 'Filtered output with each line as a separate packet' },
        ],
    })

    def perform
      filter = get_config(:filter)
      suspend and return unless filter

      IO.popen(['jq', '-c', filter], 'r+') do |io|
        # send data on input port to filter input
        inport = input(:in)
        loop do
          data = inport.read
          break unless data
          io.puts data
        end
        io.close_write

        # send filter output to output port
        outport = output(:out)
        loop do
          data = io.gets
          break unless data
          data.chomp!
          outport.write data
        end
      end
    end
  end
end
