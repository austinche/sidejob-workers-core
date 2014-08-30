# expects jq to be in the path
require 'open3'

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
      suspend unless filter

      args = ['jq', '-c']
      args << filter
      Open3.popen3(*args) do |stdin, stdout, stderr, wait_thread|
        # send data on input port to filter input
        inport = input(:in)
        inport.each do |data|
          stdin.puts data.to_json
        end
        stdin.close_write

        # send filter output to output port
        outport = output(:out)
        while data = stdout.gets do
          data = JSON.parse("[#{data.chomp}]")[0] # parses the data back from JSON while also handling primitive types
          outport.write data
        end

        err = stderr.readlines
        if err.length > 0
          raise "jq returned an error:\n#{err.join('')}"
        end
      end
    end
  end
end
