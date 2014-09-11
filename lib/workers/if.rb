module Workers
  class If
    include SideJob::Worker
    register('core', 'Workers::If', {
        description: 'Handles if-then-else logic',
        icon: 'question',
        inports: [
            { name: 'condition', type: 'boolean', description: 'Switch condition' },
            { name: 'true', type: 'all', description: 'Connected with true outport if condition is true' },
            { name: 'false', type: 'all', description: 'Connected with false outport if condition is false' },
        ],
        outports: [
            { name: 'true', type: 'all', description: 'Connected with true inport if condition is true' },
            { name: 'false', type: 'all', description: 'Connected with false inport if condition is false' },
        ],
    })

    def perform
      condition = get_config(:condition)
      if condition == true || condition == 'true'
        output(:true).write *input(:true).drain
      elsif condition == false || condition == 'false'
        output(:false).write *input(:false).drain
      else
        # anything else, we do nothing
      end
    end
  end
end
