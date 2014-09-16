module Workers
  class FilterIf
    include SideJob::Worker
    register('core', 'Workers::FilterIf', {
        description: 'Uses jq to perform conditionals',
        icon: 'question',
        inports: [
            { name: 'condition', type: 'string', description: 'jq filter that returns a boolean' },
            { name: 'true', type: 'string', description: 'jq filter that runs if condition == true' },
            { name: 'false', type: 'string', description: 'jq filter that runs if condition == false' },
            { name: 'else', type: 'string', description: 'jq filter that runs if condition is neither true nor false' },
            { name: 'vars', type: 'object', description: 'Define variables that can be used in all jq filters' },
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'true', type: 'all', description: 'Output from true filter' },
            { name: 'false', type: 'all', description: 'Output from false filter' },
            { name: 'else', type: 'all', description: 'Output from else filter' },
        ],
    })

    def perform
      return if input(:in).size == 0

      condition = get_config(:condition)
      truefilter = get_config(:true)
      falsefilter = get_config(:false)
      elsefilter = get_config(:else)
      vars = get_config(:vars)
      suspend unless condition

      inputs = input(:in).drain
      conditions = Workers::Filter.run_jq(condition, vars, inputs)
      raise 'jq returned a different number of conditions from inputs' if inputs.length != conditions.length
      
      istrue = []
      isfalse = []
      iselse = []
      inputs.each_with_index do |data, i|
        if conditions[i] == true
          istrue << data
        elsif conditions[i] == false
          isfalse << data
        else
          iselse << data
        end
      end
      output(:true).write *Workers::Filter.run_jq(truefilter, vars, istrue) if truefilter && istrue.length > 0
      output(:false).write *Workers::Filter.run_jq(falsefilter, vars, isfalse) if falsefilter && isfalse.length > 0
      output(:else).write *Workers::Filter.run_jq(elsefilter, vars, iselse) if elsefilter && iselse.length > 0
    end
  end
end
