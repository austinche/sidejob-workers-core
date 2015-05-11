require 'spec_helper'

describe Workers::Connect do
  before do
    workflow = JSON.parse('{
    "nodes": {
        "9ji5s": {
            "queue": "core",
            "class": "Workers::Object",
            "name": "Object",
            "inports": {
                "data": {},
                "n": {}
            },
            "outports": {
                "out": {}
            }
        },
        "kv83e": {
            "queue": "core",
            "class": "Workers::Connect",
            "name": "Connect",
            "inports": {
                "in": {}
            },
            "outports": {
                "out": {}
            }
        },
        "xnnf4": {
            "queue": "core",
            "class": "Workers::Wait",
            "name": "Wait",
            "inports": {
                "in": {
                    "default": [
                        1,
                        1
                    ]
                },
                "trigger": {}
            },
            "outports": {
                "out": {}
            }
        },
        "n43ca": {
            "queue": "core",
            "class": "Workers::Filter",
            "name": "Filter",
            "inports": {
                "filter": {
                    "default": "if .data[.n-1] then {result: .data[.n-1]} else {data: (.data + ([.data[-2:] | add]))} end"
                },
                "in": {}
            },
            "outports": {
                "out": {}
            }
        },
        "ib2zr": {
            "queue": "core",
            "class": "Workers::KeyValue",
            "name": "KeyValue",
            "inports": {
                "in": {}
            },
            "outports": {
                "data": {},
                "result": {}
            }
        },
        "qvzl4": {
            "queue": "core",
            "class": "Workers::SetDefault",
            "name": "SetDefault",
            "inports": {
                "in": {}
            },
            "outports": {
                "out": {}
            }
        },
        "13f69": {
            "queue": "core",
            "class": "Workers::SetDefault",
            "name": "SetDefault",
            "inports": {
                "in": {}
            },
            "outports": {
                "out": {}
            }
        }
    },
    "edges": [
        {
            "from": {
                "node": "kv83e",
                "outport": "out"
            },
            "to": {
                "node": "xnnf4",
                "inport": "trigger"
            }
        },
        {
            "from": {
                "node": "xnnf4",
                "outport": "out"
            },
            "to": {
                "node": "9ji5s",
                "inport": "data"
            }
        },
        {
            "from": {
                "node": "9ji5s",
                "outport": "out"
            },
            "to": {
                "node": "n43ca",
                "inport": "in"
            }
        },
        {
            "from": {
                "node": "n43ca",
                "outport": "out"
            },
            "to": {
                "node": "ib2zr",
                "inport": "in"
            }
        },
        {
            "from": {
                "node": "ib2zr",
                "outport": "data"
            },
            "to": {
                "node": "9ji5s",
                "inport": "data"
            }
        },
        {
            "from": {
                "node": "qvzl4",
                "outport": "out"
            },
            "to": {
                "node": "9ji5s",
                "inport": "n"
            }
        },
        {
            "from": {
                "node": "kv83e",
                "outport": "out"
            },
            "to": {
                "node": "qvzl4",
                "inport": "in"
            }
        },
        {
            "from": {
                "node": "13f69",
                "outport": "out"
            },
            "to": {
                "node": "xnnf4",
                "inport": "in"
            }
        },
        {
            "from": {
                "node": "ib2zr",
                "outport": "data"
            },
            "to": {
                "node": "13f69",
                "inport": "in"
            }
        }
    ],
    "inports": {
        "n": {
            "node": "kv83e",
            "inport": "in"
        }
    },
    "outports": {
        "result": {
            "node": "ib2zr",
            "outport": "result"
        }
    }
}')
    SideJob.redis.hset('workflows', 12345, workflow.to_json)
    @job = SideJob.queue('core', 'Workers::Workflow', args: [12345], inports: workflow['inports'], outports: workflow['outports'])
  end

  it 'calculates fibonacci numbers' do
    [1, 1, 2, 3, 5, 8, 13, 21].each_with_index do |num, n|
      @job.input(:n).write n+1
      SideJob::Worker.drain_queue
      expect(@job.output(:result).read).to eq num
    end
  end

  it 'can calculate an arbitrary fibonacci number' do
    @job.input(:n).write 6
    SideJob::Worker.drain_queue
    expect(@job.output(:result).read).to eq 8
  end
end
