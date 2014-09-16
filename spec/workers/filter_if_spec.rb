require 'spec_helper'

describe Workers::FilterIf do
  before do
    @job = SideJob.queue('core', 'Workers::FilterIf')
  end

  it 'handles true' do
    @job.input(:condition).write '. == "foo"'
    @job.input(:true).write "1"
    @job.input(:in).write "foo"
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to eq 1
    expect(@job.output(:false).size).to eq 0
    expect(@job.output(:else).size).to eq 0
  end

  it 'handles false' do
    @job.input(:condition).write '. != "foo"'
    @job.input(:false).write "2"
    @job.input(:in).write "foo"
    SideJob::Worker.drain_queue
    expect(@job.output(:true).size).to eq 0
    expect(@job.output(:false).read).to eq 2
    expect(@job.output(:else).size).to eq 0
  end

  it 'handles else' do
    @job.input(:condition).write '1'
    @job.input(:else).write "3"
    @job.input(:in).write "foo"
    SideJob::Worker.drain_queue
    expect(@job.output(:true).size).to eq 0
    expect(@job.output(:false).size).to eq 0
    expect(@job.output(:else).read).to eq 3
  end

  it 'throws away the input if no appropriate filter is given' do
    @job.input(:condition).write 'true'
    @job.input(:in).write "foo"
    SideJob::Worker.drain_queue
    expect(@job.output(:true).size).to eq 0
    expect(@job.output(:false).size).to eq 0
    expect(@job.output(:else).size).to eq 0
  end

  it 'throws error if condition filter does not return the correct number of outputs' do
    @job.input(:condition).write '1, 2'
    @job.input(:in).write "foo"
    expect { SideJob::Worker.drain_queue }.to raise_error
  end
end
