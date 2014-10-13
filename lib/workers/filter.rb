# expects jq to be in the path
require 'open3'

module Workers
  class Filter
    include SideJob::Worker
    register(
        description: 'Runs a jq filter: http://stedolan.github.io/jq/',
        icon: 'filter',
        inports: {
            filter: { mode: :memory, type: 'string', description: 'Filter in the jq language' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Filtered output with each line as a separate packet' },
        },
    )

    def perform
      for_inputs(:filter, :in) do |filter, input|
        output(:out).write *Workers::Filter.run_jq(filter, input)
      end
    end

    # Also used by FilterIf worker
    # @param filter [String]
    # @param input [Object] Will be converted to JSON before sending to jq
    # @return [Array<Object>] JSON parsed jq output
    # @raise [RuntimeError] If jq returns an error
    def self.run_jq(filter, input)
      args = ['jq', '-c']
      args << filter

      Open3.popen3(*args) do |stdin, stdout, stderr, wait_thread|
        # Send input
        stdin.puts input.to_json
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
