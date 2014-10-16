require 'spec_helper'

describe Workers::Merge do
  before do
    @job = SideJob.queue('core', 'Workers::Merge', inports: {in1: {}, in2: {}})
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:in1).write({key1: 'val1'})
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'merges objects in alphabetical order of port names' do
    [{key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'}].each {|x| @job.input(:in1).write x}
    [{key2: 'val2', common: 'y'}, {key2: 'val4', common: 'y2'}].each {|x| @job.input(:in2).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val4', 'common' => 'y2'})
  end

  it 'works with memory ports' do
    @job = SideJob.queue('core', 'Workers::Merge', inports: {in1: {mode: :memory}, in2: {}})
    [{key1: 'val0'}, {key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'}].each {|x| @job.input(:in1).write x}
    [{key2: 'val2', common: 'y'}, {key2: 'val4', common: 'y2'}].each {|x| @job.input(:in2).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val2', 'common' => 'y'})
    expect(@job.output(:out).read).to eq({'key1' => 'val3', 'key2' => 'val4', 'common' => 'y2'})
  end

  it 'suspends if missing some inputs after some merging' do
    [{key1: 'val1', common: 'x'}, {key1: 'val3', common: 'x'}].each {|x| @job.input(:in1).write x}
    @job.input(:in2).write({key2: 'val2', common: 'y'})
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
  end

  it 'resumes after suspend with partial inputs' do
    @job.input(:in1).write({key1: 'val1', common: 'x'})
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    @job.input(:in2).write({key2: 'val2', common: 'y'})
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => 'val1', 'key2' => 'val2', 'common' => 'y'})
  end
end
