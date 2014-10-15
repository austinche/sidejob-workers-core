require 'spec_helper'

describe Workers::Copy do
  before do
    @job = SideJob.queue('core', 'Workers::Copy')
  end

  it 'completes when no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'forwards one data packet' do
    data = {'test' => 123}
    @job.input(:in).write(data)
    @job.run_inline
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq data
    expect(@job.status).to eq 'completed'
  end

  it 'forwards multiple packets in order' do
    data = {'test' => 123}
    @job.input(:in).write(data)
    data2 = {'test2' => 456}
    @job.input(:in).write(data2)
    @job.run_inline
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq data
    expect(@job.output(:out).read).to eq data2
    expect(@job.status).to eq 'completed'
  end

  it 'can duplicate packets' do
    @job.input(:copies).write 3
    data = {'test' => 123}
    @job.input(:in).write(data)
    data2 = {'test2' => 456}
    @job.input(:in).write(data2)
    @job.run_inline
    expect(@job.output(:out).size).to eq 6
    3.times { expect(@job.output(:out).read).to eq data }
    3.times { expect(@job.output(:out).read).to eq data2 }
    expect(@job.status).to eq 'completed'
  end

  it 'can drop packets' do
    @job.input(:copies).write 0
    5.times { @job.input(:in).write 123 }
    @job.run_inline
    expect(@job.output(:out).size).to eq 0
    expect(@job.input(:in).size).to eq 0
    expect(@job.status).to eq 'completed'
  end
end
