require 'spec_helper'

describe Workers::Workflow do
  class Workers::TestSum
    include SideJob::Worker
    register(
        inports: {
            in: {},
        },
        outports: {
          out: {},
        },
    )
    def perform
      for_inputs(:in) do |nums|
        output(:out).write nums.inject(&:+)
      end
    end
  end

  class Workers::TestAdd
    include SideJob::Worker
    register(
        inports: {
            x: {},
            y: {},
        },
        outports: {
            out: {},
        },
    )
    def perform
      for_inputs(:x, :y) do |x, y|
        output(:out).write x + y
      end
    end
  end

  class Workers::TestDouble
    include SideJob::Worker
    register(
        inports: {
            in: {},
        },
        outports: {
          out: {},
        },
    )
    def perform
      for_inputs(:in) do |num|
        output(:out).write num * 2
      end
    end
  end

  class Workers::TestError
    include SideJob::Worker
    register(
        inports: {
            in: {},
        },
        outports: {
        },
    )
    def perform
      raise 'we have a problem'
    end
  end

  def run_graph(graph, drain_options={})
    @job = SideJob.queue('core', 'Workers::Workflow', args: [12345], inports: graph[:inports], outports: graph[:outports])
    SideJob.redis.hset('workflows', 12345, graph.to_json)
    SideJob::Worker.drain_queue(drain_options)
  end

  before do
    SideJob.redis.hset('workflows', 'empty', '{}')
  end

  describe 'runs graphs' do
    it 'can run an empty graph' do
      @job = SideJob.queue('core', 'Workers::Workflow', args: ['empty'])
      SideJob::Worker.drain_queue
      expect(@job.status).to eq 'completed'
    end

    it 'suspends on reference to non-existent workflow' do
      @job = SideJob.queue('core', 'Workers::Workflow', args: [12345])
      SideJob::Worker.drain_queue
      expect(@job.status).to eq 'suspended'
    end

    it 'fails if a child job fails' do
      run_graph({nodes: {error: {queue: 'core', class: 'Workers::TestError' }},
                 inports: {test: {node: 'error', inport: 'in'}}})
      @job.input('test').write false
      SideJob::Worker.drain_queue({errors: false})
      expect(@job.children.size).to be(1)
      expect(@job.child('error').status).to eq 'failed'
      expect(@job.status).to eq 'failed'
    end
  end

  describe '__graph port' do
    it 'can not specify workflow id and read graph from port' do
      @job = SideJob.queue('core', 'Workers::Workflow')
      @job.input(:__graph).write({})
      SideJob::Worker.drain_queue
      expect(@job.status).to eq 'completed'
    end

    it 'updates and saves new graphs' do
      @job = SideJob.queue('core', 'Workers::Workflow', args: ['empty'])
      SideJob::Worker.drain_queue
      expect(@job.get(:graph)).to eq({})
      new_graph = {nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty']}}}
      @job.input(:__graph).write(new_graph)
      SideJob::Worker.drain_queue
      expect(@job.get(:graph)).to eq(JSON.parse(new_graph.to_json))
    end

    it 'starts child jobs when graph changes' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty']}}})
      expect(@job.children.size).to be(0)
      expect(@job.status).to eq 'completed'
      @job.input(:__graph).write({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: true}}})
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(1)
      expect(@job.status).to eq 'completed'
    end

    it 'disowns child jobs that are no longer in the graph' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: true}}})
      expect(@job.children.size).to be(1)
      child = @job.child('abc')
      expect(child).to_not be nil
      expect(child.parent).to eq @job
      @job.input(:__graph).write({})
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(0)
      expect(child.parent).to be nil
      expect(@job.child('abc')).to be nil
    end
  end

  describe 'graph init' do
    it 'correctly handles initial data and starts child job' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      run_graph({nodes: {x2: { queue: 'core', class: 'Workers::TestDouble', inports: {in: {init: [4]}}}},
                 outports: {doubled: {node: 'x2', outport: 'out'}}})
      expect(@job.children.size).to be(1)
      x2 = @job.child('x2')
      expect(x2.get(:queue)).to eq 'core'
      expect(x2.get(:class)).to eq 'Workers::TestDouble'
      expect(@job.status).to eq 'completed'
      expect(SideJob.logs).to eq [{'job' => @job.id, 'timestamp' => SideJob.timestamp,
                                   'read' => [], 'write' => [{'job' => x2.id, 'inport' => 'in', 'data' => [4]}]},
                                  {'job' => x2.id, 'timestamp' => SideJob.timestamp,
                                   'read' => [{'job' => x2.id, 'inport' => 'in', 'data' => [4]}],
                                   'write' => [{'job' => x2.id, 'outport' => 'out', 'data' => [8]}],
                                  },
                                  {'job' => @job.id, 'timestamp' => SideJob.timestamp,
                                   'read' => [{'job' => x2.id, 'outport' => 'out', 'data' => [8]}],
                                   'write' => [{'job' => @job.id, 'outport' => 'doubled', 'data' => [8]}],
                                  },
                                  ]
      expect(@job.output(:doubled).read).to eq 8
    end

    it 'can send data to job outport' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum', outports: {out: {init: [5]}}},
                         double: {queue: 'core', class: 'Workers::TestDouble' }},
                 edges: [{from: {node: 'sum', outport: 'out'}, to: {node: 'double', inport: 'in'}}],
                 inports: {nums: {node: 'sum', inport: 'in'}},
                 outports: {result: {node: 'double', outport: 'out'}},
                })
      expect(@job.children.size).to be(2)
      expect(@job.status).to eq 'completed'
      expect(@job.output(:result).read).to eq 10
    end

    it 'sends multiple initial data' do
      run_graph({nodes: {x2: { queue: 'core', class: 'Workers::TestDouble', inports: {in: {init: [4, 5]}}}},
                 outports: {doubled: {node: 'x2', outport: 'out'}}})
      expect(@job.children.size).to be(1)
      expect(@job.status).to eq 'completed'
      expect(@job.output(:doubled).read).to eq 8
      expect(@job.output(:doubled).read).to eq 10
    end

    it 'can initialize a child job without sending data' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: true}}})
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(1)
      child = @job.child('abc')
      expect(child.get(:queue)).to eq 'core'
      expect(child.get(:class)).to eq 'Workers::Workflow'
      expect(child.get(:args)).to eq ['empty']
    end

    it 'can adopt a child job instead of starting a new one' do
      child = SideJob.queue('core', 'Workers::Workflow', args: ['empty'])
      expect(child.parent).to be nil
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: child.id}}})
      expect(@job.children.size).to be(1)
      expect(child.parent).to eq @job
    end

    it 'raises error if child job is specified but node is already started' do
      child = SideJob.queue('core', 'Workers::Workflow', args: ['empty'])
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: true}}})
      @job.input(:__graph).write({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: child.id}}})
      expect { SideJob::Worker.drain_queue }.to raise_error
    end

    it 'raises errors if child job to be adopted params does not match node' do
      expect { run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: 123}}}) }.to raise_error
      expect { run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'],
                                        init: SideJob.queue('wrong', 'Workers::Workflow', args: ['empty']).id}}})
      }.to raise_error
      expect { run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'],
                          init: SideJob.queue('core', 'wrong', args: ['empty']).id}}})
      }.to raise_error
      expect { run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'],
                          init: SideJob.queue('core', 'Workers::Workflow', args: ['wrong']).id}}})
      }.to raise_error
    end
  end

  describe 'graph ports' do
    it 'handles inports and outports' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum' }},
                 inports: {nums: {node: 'sum', inport: 'in'}},
                 outports: {total: {node: 'sum', outport: 'out'}},
                })
      @job.input(:nums).write [1, 2, 3, 4]
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(1)
      expect(@job.status).to eq 'completed'
      expect(@job.output(:total).read).to eq 10
    end

    it 'uses default values from inports' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestAdd' }},
                 inports: {x: {node: 'sum', inport: 'x', default: 5}, y: {node: 'sum', inport: 'y'}},
                 outports: {total: {node: 'sum', outport: 'out'}},
                })
      @job.input(:y).write 7
      SideJob::Worker.drain_queue
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(1)
      expect(@job.output(:total).read).to eq 12
    end

    it 'updates inports and outports from a new graph' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum' }}})
      expect(@job.inports.size).to eq 1 # __graph port
      expect(@job.outports.size).to eq 0
      @job.input(:__graph).write({nodes: {sum: {queue: 'core', class: 'Workers::TestSum' }},
                 inports: {nums: {node: 'sum', inport: 'in'}},
                 outports: {total: {node: 'sum', outport: 'out'}},
                })
      @job.input(:nums).write [1, 2, 3, 4]
      SideJob::Worker.drain_queue
      expect(@job.inports.size).to eq 2
      expect(@job.outports.size).to eq 1
      expect(@job.output(:total).read).to eq 10
    end
    
    it 'does not start a child job connected to a graph inport unless there is input' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum' }},
                 inports: {nums: {node: 'sum', inport: 'in'}},
                 outports: {total: {node: 'sum', outport: 'out'}},
                })
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(0)
      @job.input(:nums).write [1, 2, 3]
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(1)
    end
  end

  describe 'graph nodes' do
    it 'does not start child workflow if no input' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'] }}})
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(0)
    end

    it 'does not start a child job unless there is input from an upstream job' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: true},
                         xyz: {queue: 'core', class: 'Workers::TestDouble'}},
                 edges: [{from: {node: 'abc', outport: 'out'}, to: {node: 'xyz', inport: 'in'}}],
                })
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(1)
      child = @job.child('abc')
      expect(child.get(:queue)).to eq 'core'
      expect(child.get(:class)).to eq 'Workers::Workflow'
      expect(child.get(:args)).to eq ['empty']
    end

    it 'only starts a child job once' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], inports: {in: {init: [1]}}}}})
      @job.run
      expect(@job.status).to eq 'queued'
      SideJob::Worker.drain_queue
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(1)
    end

    it 'starts child job with port options' do
      run_graph({nodes: {abc: {queue: 'core', class: 'Workers::Workflow', args: ['empty'], init: true, inports: {myport: {default: 123}}}}})
      @job.run
      expect(@job.status).to eq 'queued'
      SideJob::Worker.drain_queue
      expect(@job.status).to eq 'completed'
      expect(@job.child(:abc).input(:myport).default).to eq(123)
    end
  end

  describe 'graph edges' do
    it 'handles connections between jobs' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum' },
                         double: {queue: 'core', class: 'Workers::TestDouble' }},
                 edges: [{from: {node: 'sum', outport: 'out'}, to: {node: 'double', inport: 'in'}}],
                 inports: {nums: {node: 'sum', inport: 'in'}},
                 outports: {result: {node: 'double', outport: 'out'}},
                })
      @job.input(:nums).write [1, 2, 3, 4]
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(2)
      expect(@job.status).to eq 'completed'
      expect(@job.output(:result).read).to eq 20
    end

    it 'sends data to outport correctly if another job also uses the output' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum' },
                         double: {queue: 'core', class: 'Workers::TestDouble' },
                         double2: {queue: 'core', class: 'Workers::TestDouble' }},
                edges: [
                     {from: {node: 'sum', outport: 'out'}, to: {node: 'double', inport: 'in'}},
                     {from: {node: 'double', outport: 'out'}, to: {node: 'double2', inport: 'in'}},
                 ],
                 inports: {nums: {node: 'sum', inport: 'in'}},
                 outports: {doubled: {node: 'double', outport: 'out'}, quadrupled: {node: 'double2', outport: 'out'}},
                })
      @job.input(:nums).write [1, 2, 3, 4]
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(3)
      expect(@job.status).to eq 'completed'
      expect(@job.output(:doubled).read).to eq 20
      expect(@job.output(:quadrupled).read).to eq 40
    end

    it 'connects port defaults' do
      run_graph({nodes: {sum: {queue: 'core', class: 'Workers::TestSum', init: true, outports: {out: {default: 10}}},
                         add: {queue: 'core', class: 'Workers::TestAdd' }},
                 edges: [{from: {node: 'sum', outport: 'out'}, to: {node: 'add', inport: 'x'}}],
                 inports: {y: {node: 'add', inport: 'y'}},
                 outports: {result: {node: 'add', outport: 'out'}},
                })
      @job.input(:y).write 15
      SideJob::Worker.drain_queue
      expect(@job.children.size).to be(2)
      expect(@job.output(:result).read).to eq 25
      expect(@job.status).to eq 'completed'
    end
  end
end
