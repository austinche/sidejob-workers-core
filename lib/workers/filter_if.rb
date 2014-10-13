module Workers
  class FilterIf
    include SideJob::Worker
    register(
        description: 'Uses jq to perform conditionals: http://stedolan.github.io/jq/',
        icon: 'question',
        inports: {
            condition: { mode: :memory, type: 'string', description: 'jq filter that returns a boolean' },
            true: { mode: :memory, type: 'string', description: 'jq filter that runs if condition == true' },
            false: { mode: :memory, type: 'string', description: 'jq filter that runs if condition == false' },
            else: { mode: :memory, type: 'string', description: 'jq filter that runs if condition is neither true nor false' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            true: { type: 'all', description: 'Output from true filter' },
            false: { type: 'all', description: 'Output from false filter' },
            else: { type: 'all', description: 'Output from else filter' },
        },
    )

    def perform
      for_inputs(:condition, :true, :false, :else, :in) do |condition, truefilter, falsefilter, elsefilter, input|
        result = Workers::Filter.run_jq(condition, input)
        raise 'jq condition filter did not return a single result' if result.length != 1
        if result[0] == true
          output(:true).write *Workers::Filter.run_jq(truefilter, input)
        elsif result[0] == false
          output(:false).write *Workers::Filter.run_jq(falsefilter, input)
        else
          output(:else).write *Workers::Filter.run_jq(elsefilter, input)
        end
      end
    end
  end
end
