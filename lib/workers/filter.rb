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
            { name: 'vars', type: 'object', description: 'Define variables that can be used in the jq filter' },
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

      vars = get_config(:vars)
      suspend if vars === true # wait for real vars

      output(:out).write *Workers::Filter.run_jq(filter, vars, input(:in).drain)
    end

    # @param filter [String]
    # @param vars [Hash]
    # @param inputs [Array<Object>] Will be converted to JSON before sending to jq
    # @return [Array<Object>] JSON parsed jq output
    # @raise [RuntimeError] If jq returns an error
    def self.run_jq(filter, vars, inputs)
      args = ['jq', '-c']
      if vars
        vars.each_pair do |name, val|
          args.concat ['--arg', name, val.to_json]
        end
      end
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
