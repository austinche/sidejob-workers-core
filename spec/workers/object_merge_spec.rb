require 'spec_helper'

describe Workers::ObjectMerge do
  before do
    @job = SideJob.queue('core', 'Workers::ObjectMerge')
  end

  it 'completes on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:in1).write({key1: 'val1'})
    @job.input(:in2).write nil
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'ignores nulls' do
    @job.input(:in1).write({key1: 'val1'})
    @job.input(:in2).write nil, {key2: 'val2'}, nil
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2'})
  end

  it 'merges objects in alphabetical order of port names' do
    @job.input(:in1).write({key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'})
    @job.input(:in2).write({key2: 'val2', common: 'y'}, {key2: 'val4', common: 'y2'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val4', 'common' => 'y2'})
  end

  it 'suspends if missing some inputs after some merging' do
    @job.input(:in1).write({key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'})
    @job.input(:in2).write({key2: 'val2', common: 'y'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
  end

  it 'saves and resumes with partially read inputs' do
    @job.input(:in1).write({key1: 'val1', common: 'x'})
    @job.input(:in2).write nil
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    @job.input(:in2).write({key2: 'val2', common: 'y'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
  end
end
