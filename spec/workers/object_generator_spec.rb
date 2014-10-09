require 'spec_helper'

describe Workers::ObjectGenerator do
  before do
    @job = SideJob.queue('core', 'Workers::ObjectGenerator')
  end

  it 'suspends on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'completes on config but no input' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1', common: 'x'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'merges objects by default' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'})
    @job.input(:in2).write({key2: 'val2', common: 'y'}, {key2: 'val4', common: 'y2'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val4', 'common' => 'y2'})
  end

  it 'raises error if port is not specified in config' do
    @job.input(:config).write [{},]
    expect { SideJob::Worker.drain_queue }.to raise_error
  end

  it 'suspends if missing some inputs after some merging' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'})
    @job.input(:in2).write({key2: 'val2', common: 'y'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
  end

  it 'saves and resumes with partially read inputs' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1', common: 'x'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    @job.input(:in2).write({key2: 'val2', common: 'y'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
  end

  it 'specify a port should use most recent data' do
    @job.input(:config).write [{port: 'in1', use_recent: true}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'})
    @job.input(:in2).write({key2: 'val2', common: 'y'}, {key2: 'val4', common: 'y2'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val2', 'common' => 'y'})
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val4', 'common' => 'y2'})
  end

  it 'can specify a key to generate a new object' do
    @job.input(:config).write [{port: 'in1', use_recent: true, key: 'x'}, {port: 'in2', key: 'y'}]
    @job.input(:in1).write({key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'})
    @job.input(:in2).write({key2: 'val2', common: 'y'}, {key2: 'val4', common: 'y2'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'x' => {'key1' => 'val3', 'common' => 'x'}, 'y' => {'key2' => 'val2', 'common' => 'y'}})
    expect(@job.output(:out).read).to eq({'x' => {'key1' => 'val3', 'common' => 'x'}, 'y' => {'key2' => 'val4', 'common' => 'y2'}})
  end

  it 'can convert from an array' do
    @job.input(:config).write [{port: 'in1', use_recent: true, from_array: true, key: 'x'}, {port: 'in2', from_array: true, key: 'y'}]
    @job.input(:in1).write([['key1', 'val1'], ['common', 'x']], [['key1', 'val3'], ['common', 'x']])
    @job.input(:in2).write([['key2', 'val2'], ['common', 'y']], [['key2', 'val4'], ['common', 'y2']])
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'x' => {'key1' => 'val3', 'common' => 'x'}, 'y' => {'key2' => 'val2', 'common' => 'y'}})
    expect(@job.output(:out).read).to eq({'x' => {'key1' => 'val3', 'common' => 'x'}, 'y' => {'key2' => 'val4', 'common' => 'y2'}})
  end
end
