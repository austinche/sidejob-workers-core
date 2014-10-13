require 'spec_helper'

describe Workers::FilterIf do
  before do
    @job = SideJob.queue('core', 'Workers::FilterIf')
    @job.input(:true).write '1'
    @job.input(:false).write '2'
    @job.input(:else).write '3'
    @job.input(:in).write 'foo'
  end

  it 'handles true' do
    @job.input(:condition).write '. == "foo"'
    SideJob::Worker.drain_queue
    expect(@job.output(:true).read).to eq 1
    expect(@job.output(:false).data?).to be false
    expect(@job.output(:else).data?).to be false
  end

  it 'handles false' do
    @job.input(:condition).write '. != "foo"'
    SideJob::Worker.drain_queue
    expect(@job.output(:true).data?).to be false
    expect(@job.output(:false).read).to eq 2
    expect(@job.output(:else).data?).to be false
  end

  it 'handles else' do
    @job.input(:condition).write '1'
    SideJob::Worker.drain_queue
    expect(@job.output(:true).data?).to be false
    expect(@job.output(:false).data?).to be false
    expect(@job.output(:else).read).to eq 3
  end

  it 'throws error if condition filter does not return the correct number of outputs' do
    @job.input(:condition).write '1, 2'
    @job.input(:in).write "foo"
    expect { SideJob::Worker.drain_queue }.to raise_error
  end
end
