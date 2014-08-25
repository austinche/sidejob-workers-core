require 'spec_helper'

describe Workers::Workflow do
  def stub_workflow(id, graph)
    workflow = {
      'found' => true,
      '_source' => {
        'graph' => JSON.generate(graph),
      },
    }
    stub_request(:get, "http://localhost:9200/workflows/workflow/#{id}").to_return(status: 200, body: workflow)
  end

  before do
    stub_workflow('empty', {processes: {}, connections: []})
    stub_workflow('123', {processes: {'abc' => { 'component' => 'workflow/empty' } }, connections: []})
  end

  it 'can run an empty graph' do
    job = SideJob.queue('core', 'Workers::Workflow', {args: ['empty']})
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
  end

  it 'fails on non-existent workflow' do
    stub_request(:get, "http://localhost:9200/workflows/workflow/missing").to_return(status: 404, body: {'found' => false})
    job = SideJob.queue('core', 'Workers::Workflow', {args: ['missing']})
    expect{ Timeout::timeout(5) { Sidekiq::Worker.drain_all } }.to raise_error
  end

  it 'can start a child workflow' do
    job = SideJob.queue('core', 'Workers::Workflow', {args: [123]})
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
    expect(job.children.size).to be(1)
    expect(job.children[0].queue_name).to eq('core')
    expect(job.children[0].class_name).to eq('Workers::Workflow')
  end

  it 'only starts a child job once' do
    job = SideJob.queue('core', 'Workers::Workflow', {args: [123]})
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    job.restart
    expect(job.status).to be(:queued)
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
    expect(job.children.size).to be(1)
  end

  it 'starts a child job if graph changes' do
    job = SideJob.queue('core', 'Workers::Workflow', {args: ['empty']})
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.children.size).to be(0)
    expect(job.status).to be(:completed)
    job.set_json :graph, {processes: {'abc' => { 'component' => 'workflow/empty' } }, connections: []}
    job.restart
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.children.size).to be(1)
    expect(job.status).to be(:completed)
  end

  it 'correctly handles inports and outports' do
    stub_workflow('sum', {
                    processes: {'sum' => { 'component' => 'core/Workers::Filter' }},
                    connections: [{'data' => 'add', 'tgt' => {'process' => 'sum', 'port' => 'filter'}}],
                    inports: {'nums' => {'process' => 'sum', 'port' => 'in'}},
                    outports: {'total' => {'process' => 'sum', 'port' => 'out'}},
                  })
    job = SideJob.queue('core', 'Workers::Workflow', {args: ['sum']})
    job.input(:nums).push_json [1, 2, 3, 4]
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.children.size).to be(1)
    expect(job.status).to be(:completed)
    expect(job.output(:total).pop.to_i).to eq 10
  end

  it 'handles connections between jobs' do
    stub_workflow('111', {
                    processes: {'sum' => { 'component' => 'core/Workers::Filter' }, 'double' => { 'component' => 'core/Workers::Filter' }}, 
                    connections: [{'data' => 'add', 'tgt' => {'process' => 'sum', 'port' => 'filter'}}, {'data' => '. * 2', 'tgt' => {'process' => 'double', 'port' => 'filter'}},
                                  {'src' => {'process' => 'sum', 'port' => 'out'}, 'tgt' => {'process' => 'double', 'port' => 'in'}},
                                 ],
                    inports: {'nums' => {'process' => 'sum', 'port' => 'in'}},
                    outports: {'result' => {'process' => 'double', 'port' => 'out'}},
                  })
    job = SideJob.queue('core', 'Workers::Workflow', {args: ['111']})
    job.input(:nums).push_json [1, 2, 3, 4]
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.children.size).to be(2)
    expect(job.status).to be(:completed)
    expect(job.output(:result).pop.to_i).to eq 20
  end

  it 'sends data to outport correctly if another job also uses the output' do
    stub_workflow('111', {
                    processes: {'sum' => { 'component' => 'core/Workers::Filter' }, 'double' => { 'component' => 'core/Workers::Filter' }, 'plus1' => { 'component' => 'core/Workers::Filter' }}, 
                    connections: [{'data' => 'add', 'tgt' => {'process' => 'sum', 'port' => 'filter'}}, 
                                  {'data' => '. * 2', 'tgt' => {'process' => 'double', 'port' => 'filter'}},
                                  {'data' => '. + 1', 'tgt' => {'process' => 'plus1', 'port' => 'filter'}},
                                  {'src' => {'process' => 'sum', 'port' => 'out'}, 'tgt' => {'process' => 'double', 'port' => 'in'}},
                                  {'src' => {'process' => 'double', 'port' => 'out'}, 'tgt' => {'process' => 'plus1', 'port' => 'in'}},
                                 ],
                    inports: {'nums' => {'process' => 'sum', 'port' => 'in'}},
                    outports: {'doubled' => {'process' => 'double', 'port' => 'out'}, 'final' => {'process' => 'plus1', 'port' => 'out'}},
                  })
    job = SideJob.queue('core', 'Workers::Workflow', {args: ['111']})
    job.input(:nums).push_json [1, 2, 3, 4]
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.children.size).to be(3)
    expect(job.status).to be(:completed)
    expect(job.output(:doubled).pop.to_i).to eq 20
    expect(job.output(:final).pop.to_i).to eq 21
  end
end
