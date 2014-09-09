require 'spec_helper'

describe Workers::Group do
  before do
    @job = SideJob.queue('core', 'Workers::Group')
  end

  it 'suspends on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'suspends until n data have arrived' do
    @job.input(:n).write 3
    @job.input(:in).write 1, 2
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'sends out array of n data' do
    @job.input(:n).write 3
    @job.input(:in).write 'a', 'b', 'c'
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
  end

  it 'handles a multiple of n data' do
    @job.input(:n).write 3
    @job.input(:in).write 'a', 'b', 'c', 1, 2, 3
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    expect(@job.output(:out).read).to eq [1, 2, 3]
  end

  it 'handles a non-multiple of n data' do
    @job.input(:n).write 3
    @job.input(:in).write 'a', 'b', 'c', 'd'
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    expect(@job.output(:out).read).to be nil
  end

  it 'handles data over time' do
    @job.input(:n).write 3
    @job.input(:in).write 'a', 'b'
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    @job.input(:in).write 'c'
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
  end

  it 'properly persists unwritten stored data' do
    @job.input(:n).write 3
    @job.input(:in).write 'a'
    SideJob::Worker.drain_queue
    @job.input(:in).write 'b', 'c'
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    @job.input(:in).write 'd', 'e', 'f'
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq ['d', 'e', 'f']
  end
end
