# expects jq to be in the path
require 'open3'

module Workers
  class Filter
    include SideJob::Worker
    register('core', 'Workers::Filter', {
        description: 'Runs a jq filter: http://stedolan.github.io/jq/',
        icon: 'filter',
        inports: [
            { name: 'filter', type: 'string', description: 'Filter in the jq language' },
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'all', description: 'Filtered output with each line as a separate packet' },
        ],
    })

    def perform
      return if input(:in).size == 0

      filter = get_config(:filter)
      suspend unless filter

      output(:out).write *Workers::Filter.run_jq(filter, input(:in).drain)
    end

    # @param filter [String]
    # @param inputs [Array<Object>] Will be converted to JSON before sending to jq
    # @return [Array<Object>] JSON parsed jq output
    # @raise [RuntimeError] If jq returns an error
    def self.run_jq(filter, inputs)
      args = ['jq', '-c']
      args << filter

      Open3.popen3(*args) do |stdin, stdout, stderr, wait_thread|
        # Send inputs
        inputs.each do |data|
          stdin.puts data.to_json
        end
        stdin.close_write

        # Check errors
        err = stderr.readlines
        raise "jq returned an error:\n#{err.join('')}" if err.length > 0

        # Read outputs and convert from json
        return stdout.readlines.map do |data|
          JSON.parse("[#{data.chomp}]")[0] # parses the data back from JSON while also handling primitive types
        end
      end
    end
  end
end
