require 'spec_helper'

describe Workers::SetDefault do
  before do
    @job = SideJob.queue('core', 'Workers::SetDefault')
  end

  it 'completes when no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'sets default to last value received' do
    3.times {|i| @job.input(:in).write i}
    @job.run_inline
    expect(@job.output(:out).default).to eq 2
    expect(@job.output(:out).size).to eq 0
    expect(@job.output(:out).read).to eq 2
    expect(@job.status).to eq 'completed'
  end

  it 'can overwrite default value' do
    [true, false, nil, 5, 'abc'].each do |x|
      @job.input(:in).write x
      @job.run_inline
      expect(@job.output(:out).default).to eq x
    end
  end

  it 'uses input default value over any data' do
    @job.input(:in).default = 1234
    @job.input(:in).write [6, 7]
    @job.run_inline
    expect(@job.output(:out).default).to eq 1234
    expect(@job.output(:out).size).to eq 0
  end
end
