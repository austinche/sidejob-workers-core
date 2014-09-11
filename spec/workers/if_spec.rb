require 'spec_helper'

describe Workers::Filter do
  before do
    @job = SideJob.queue('core', 'Workers::If')
    @job.input(:true).write 'istrue'
    @job.input(:false).write 'isfalse'
  end

  it 'completes when no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'recognizes boolean true as true' do
    @job.input(:condition).write true
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to eq 'istrue'
    expect(@job.output(:false).read).to be nil
  end

  it 'recognizes true string as true' do
    @job.input(:condition).write 'true'
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to eq 'istrue'
    expect(@job.output(:false).read).to be nil
  end

  it 'recognizes boolean false as false' do
    @job.input(:condition).write false
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to be nil
    expect(@job.output(:false).read).to eq 'isfalse'
  end

  it 'recognizes false string as false' do
    @job.input(:condition).write 'false'
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to be nil
    expect(@job.output(:false).read).to eq 'isfalse'
  end

  it 'anything else is ignored' do
    @job.input(:condition).write '2'
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to be nil
    expect(@job.output(:false).read).to be nil
  end
end
