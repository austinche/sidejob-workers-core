module Workers
  class Permutation
    include SideJob::Worker
    register(
        description: 'Calculates permutations or combinations on an array.',
        icon: 'th',
        inports: {
            ordered: { type: 'boolean', description: 'True for permutations, false for combinations', default: false },
            repeat: { type: 'boolean', description: 'Allow repeats?', default: false },
            n: { type: 'integer', description: 'Length' },
            in: { type: 'array', description: 'Input array' },
        },
        outports: {
            out: { type: 'array', description: 'Output array of arrays in an undefined order' },
        },
    )

    def perform
      for_inputs(:ordered, :repeat, :n, :in) do |ordered, repeat, n, input|
        if ordered
          if repeat
            output(:out).write input.repeated_permutation(n).to_a
          else
            output(:out).write input.permutation(n).to_a
          end
        else
          if repeat
            output(:out).write input.repeated_combination(n).to_a
          else
            output(:out).write input.combination(n).to_a
          end
        end
      end
    end
  end
end
