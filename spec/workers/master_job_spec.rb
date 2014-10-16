require 'spec_helper'

describe Workers::MasterJob do
  before do
    @job = SideJob.queue('core', 'Workers::MasterJob')
  end

  it 'raises an error if inport is memory port or has default value' do
    @job = SideJob.queue('core', 'Workers::MasterJob', inports: {inport: {mode: :memory}})
    expect { @job.run_inline }.to raise_error
    @job = SideJob.queue('core', 'Workers::MasterJob', inports: {inport: {default: true}})
    expect { @job.run_inline }.to raise_error
  end

  it 'raises an error if queue is memory port or has default value' do
    @job = SideJob.queue('core', 'Workers::MasterJob', inports: {queue: {mode: :memory}})
    expect { @job.run_inline }.to raise_error
    @job = SideJob.queue('core', 'Workers::MasterJob', inports: {queue: {default: true}})
    expect { @job.run_inline }.to raise_error
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'can queue a child job' do
    @job.input(:queue).write({ queue: 'core', class: 'Workers::Filter' })
    expect(@job.children.size).to eq 0
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.children.size).to eq 1
    child = @job.children[0]
    expect(child.get(:queue)).to eq 'core'
    expect(child.get(:class)).to eq 'Workers::Filter'
  end

  it 'can queue a child job with extra options' do
    at = Time.now.to_f + 1000
    @job.input(:queue).write({ queue: 'core', class: 'Workers::MasterJob', at: at })
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.children.size).to eq 1
    child = @job.children[0]
    expect(Sidekiq::ScheduledSet.new.find_job(child.jid).at).to eq(Time.at(at))
  end

  it 'can queue a child job with a name and send data to inport' do
    @job.input(:queue).write({ name: 'myjob', queue: 'core', class: 'Workers::Placeholder' })
    @job.input(:inport).write({ name: 'myjob', port: 'unread', data: {foo: 'bar'} })
    @job.run_inline
    expect(@job.status).to eq 'completed'
    child = @job.children[0]
    expect(child.input(:unread).size).to eq 1
    expect(child.input(:unread).read).to eq({'foo' => 'bar' })
  end

  it 'raises an error if try sending to an unknown named job' do
    @job.input(:inport).write({ name: 'myjob', port: 'unread', data: {foo: 'bar'} })
    expect { @job.run_inline }.to raise_error
  end

  it 'can queue a child job and forward on outport data' do
    @job.input(:queue).write({ name: 'myjob', queue: 'core', class: 'Workers::Placeholder' })
    @job.run_inline
    child = @job.children[0]
    child.output(:test).write({foo: 'bar'})
    expect(@job.output(:outport).size).to eq 0
    @job.run_inline
    expect(@job.output(:outport).size).to eq 1
    expect(@job.output(:outport).read).to eq({ 'name' => 'myjob', 'id' => child.jid, 'port' => 'test', 'data' => {'foo' => 'bar'}})
  end

  it 'integration test' do
    @job.input(:queue).write({ name: 'myjob', queue: 'core', class: 'Workers::Filter' })
    @job.input(:inport).write({ name: 'myjob', port: 'filter', data: '.foo' })
    @job.input(:inport).write({ name: 'myjob', port: 'in', data: {foo: 'bar'} })
    SideJob::Worker.drain_queue
    child = @job.children[0]
    expect(@job.output(:outport).size).to eq 1
    expect(@job.output(:outport).read).to eq({ 'name' => 'myjob', 'id' => child.jid, 'port' => 'out', 'data' => 'bar'})
  end
end
