require 'spec_helper'

describe Workers::Store do
  before do
    @job = SideJob.queue('core', 'Workers::Store')
  end

  it 'completes when no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'does not send data unless it is asked for' do
    @job.input(:store).write(123)
    SideJob::Worker.drain_queue
    expect(@job.output('data').size).to be 0
    expect(@job.status).to eq 'completed'
  end

  it 'retrieves null if nothing is stored' do
    @job.input(:retrieve).write true
    SideJob::Worker.drain_queue
    expect(@job.output('data').size).to be 1
    expect(@job.output('data').read).to be nil
    expect(@job.status).to eq 'completed'
  end

  it 'can store and retrieve the same data multiple times' do
    @job.input(:store).write(123)
    @job.input(:retrieve).write true, false
    SideJob::Worker.drain_queue
    expect(@job.output('data').size).to be 2
    expect(@job.output('data').drain).to eq [123, 123]
    expect(@job.status).to eq 'completed'
  end

  it 'data is overwritten by later stores' do
    @job.input(:store).write(123)
    @job.input(:retrieve).write true
    SideJob::Worker.drain_queue
    @job.input(:store).write([1, 2])
    @job.input(:retrieve).write 1
    SideJob::Worker.drain_queue
    expect(@job.output('data').size).to be 2
    expect(@job.output('data').drain).to eq [123, [1,2]]
    expect(@job.status).to eq 'completed'
  end
end
