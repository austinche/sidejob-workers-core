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

  def stub_workflow(workflow_id, graph)
    stub_request(:get, "http://localhost:9200/workflows/workflow/#{workflow_id}/_source").to_return(status: 200, body: {'graph' => graph.to_json})
  end

  def stub_job(job_id, graph)
    stub_request(:get, "http://localhost:9200/jobs/job/#{job_id}/_source").to_return(status: 200, body: {'graph' => graph.to_json})
  end

  def run_graph(graph)
    @job = SideJob.queue('core', 'Workers::Workflow', args: [12345], inports: graph[:inports], outports: graph[:outports])
    stub_workflow 12345, graph
    SideJob::Worker.drain_queue
    @job.reload
  end

  before do
    stub_request(:get, %r{http://localhost:9200/jobs/job.*}).to_return(status: 200, body: {})
    stub_request(:post, %r{http://localhost:9200/jobs/job.*}).to_return(status: 200, body: {})
    stub_request(:get, %r{http://localhost:9200/workflows/workflow/.*/_source}).to_return(status: 200, body: false)
    stub_workflow 123, {}
  end

  it 'can run an empty graph' do
    run_graph({})
    expect(@job.status).to eq 'completed'
  end

  it 'fails on reference to non-existent workflow' do
    @job = SideJob.queue('core', 'Workers::Workflow', args: [12345])
    expect { SideJob::Worker.drain_queue }.to raise_error
    @job.reload
    expect(@job.status).to eq 'failed'
  end

  describe 'child jobs' do
    it 'does not start child workflow if no input' do
      run_graph({processes: {'abc' => { 'metadata' => {'queue' => 'core', 'class' => 'Workers::Workflow', 'args' => [123] } }}, connections: []})
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(0)
    end

    it 'starts child job if there is initial input' do
      run_graph({processes: {'abc' => { 'metadata' => {
          'queue' => 'core', 'class' => 'Workers::Workflow', 'args' => [123],
          'inports' => {'in' => {'data' => [1]}}
      } }}, connections: []})
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(1)
      child = @job.child('abc')
      expect(child.get(:queue)).to eq 'core'
      expect(child.get(:class)).to eq 'Workers::Workflow'
      expect(child.get(:args)).to eq [123]
    end

    it 'only starts a child job once' do
      run_graph({processes: {'abc' => { 'metadata' => {
          'queue' => 'core', 'class' => 'Workers::Workflow', 'args' => [123],
          'inports' => {'in' => {'data' => [1]}}
      } }}, connections: []})
      @job.run
      expect(@job.status).to eq 'queued'
      SideJob::Worker.drain_queue
      @job.reload
      expect(@job.status).to eq 'completed'
      expect(@job.children.size).to be(1)
    end

    it 'handles starting a child job when graph changes' do
      run_graph({})
      expect(@job.children.size).to be(0)
      expect(@job.status).to eq 'completed'
      stub_job @job.id, {processes: {'abc' => { 'metadata' => {
          'queue' => 'core', 'class' => 'Workers::Workflow', 'args' => [123],
          'inports' => {'in' => {'data' => []}}
      }}}, connections: []}
      @job.run
      SideJob::Worker.drain_queue
      @job.reload
      expect(@job.children.size).to be(1)
      expect(@job.status).to eq 'completed'
    end
  end

  it 'correctly handles initial data' do
    now = Time.now
    allow(Time).to receive(:now) { now }
    run_graph({processes: {'x2' => { 'metadata' => {
        'queue' => 'core', 'class' => 'Workers::TestDouble',
        'inports' => { 'in' => {'data' => [4]}}
    } }}, connections: [], outports: {'doubled' => {'process' => 'x2', 'port' => 'out'}}})
    expect(@job.children.size).to be(1)
    x2 = @job.child('x2')
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

  it 'sends multiple initial data' do
    run_graph({processes: {'x2' => { 'metadata' => {
        'queue' => 'core', 'class' => 'Workers::TestDouble',
        'inports' => { 'in' => {'data' => [4,5]}}
    } }}, connections: [], outports: {'doubled' => {'process' => 'x2', 'port' => 'out'}}})
    expect(@job.children.size).to be(1)
    expect(@job.status).to eq 'completed'
    expect(@job.output(:doubled).read).to eq 8
    expect(@job.output(:doubled).read).to eq 10
  end

  it 'correctly handles inports and outports' do
    run_graph({processes: {'sum' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestSum' }}},
               inports: {'nums' => {'process' => 'sum', 'port' => 'in'}},
               outports: {'total' => {'process' => 'sum', 'port' => 'out'}},
              })
    @job.input(:nums).write [1, 2, 3, 4]
    SideJob::Worker.drain_queue
    @job.reload
    expect(@job.children.size).to be(1)
    expect(@job.status).to eq 'completed'
    expect(@job.output(:total).read).to eq 10
  end

  it 'handles connections between jobs' do
    run_graph({processes: {
        'sum' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestSum' }},
        'double' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestDouble' }}},
               connections: [{'src' => {'process' => 'sum', 'port' => 'out'}, 'tgt' => {'process' => 'double', 'port' => 'in'}}],
               inports: {'nums' => {'process' => 'sum', 'port' => 'in'}},
               outports: {'result' => {'process' => 'double', 'port' => 'out'}},
              })
    @job.input(:nums).write [1, 2, 3, 4]
    SideJob::Worker.drain_queue
    @job.reload
    expect(@job.children.size).to be(2)
    expect(@job.status).to eq 'completed'
    expect(@job.output(:result).read).to eq 20
  end

  it 'sends data to outport correctly if another job also uses the output' do
    run_graph({processes: {
        'sum' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestSum' }},
        'double' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestDouble' }},
        'double2' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestDouble' }}},
               connections: [
                   {'src' => {'process' => 'sum', 'port' => 'out'}, 'tgt' => {'process' => 'double', 'port' => 'in'}},
                   {'src' => {'process' => 'double', 'port' => 'out'}, 'tgt' => {'process' => 'double2', 'port' => 'in'}},
               ],
               inports: {'nums' => {'process' => 'sum', 'port' => 'in'}},
               outports: {'doubled' => {'process' => 'double', 'port' => 'out'},
                          'quadrupled' => {'process' => 'double2', 'port' => 'out'}},
              })
    @job.input(:nums).write [1, 2, 3, 4]
    SideJob::Worker.drain_queue
    @job.reload
    expect(@job.children.size).to be(3)
    expect(@job.status).to eq 'completed'
    expect(@job.output(:doubled).read).to eq 20
    expect(@job.output(:quadrupled).read).to eq 40
  end

  it 'fails if a child job fails' do
    @job = SideJob.queue('core', 'Workers::Workflow', args: [12345], inports: {test: {}})
    stub_workflow(12345, {processes: {
        'error' => { 'metadata' => { 'queue' => 'core', 'class' => 'Workers::TestError' }}},
                          inports: {'test' => {'process' => 'error', 'port' => 'in'}},
    })
    @job.input(:test).write true
    SideJob::Worker.drain_queue(errors: false)
    @job.reload
    expect(@job.children.size).to be(1)
    expect(@job.child('error').status).to eq 'failed'
    expect(@job.status).to eq 'failed'
  end
end
