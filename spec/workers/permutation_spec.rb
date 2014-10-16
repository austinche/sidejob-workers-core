require 'spec_helper'

describe Workers::Permutation do
  before do
    @job = SideJob.queue('core', 'Workers::Permutation')
    @data = [1,2,3,4]
  end

  it 'calculates combinations by default' do
    @job.input(:in).write @data
    @job.input(:n).write 2
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq @data.combination(2).to_a
  end

  it 'can calculate repeated combinations' do
    @job.input(:repeat).write true
    @job.input(:in).write @data
    @job.input(:n).write 2
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq @data.repeated_combination(2).to_a
  end

  it 'can calculate permutations' do
    @job.input(:ordered).write true
    @job.input(:repeat).write false
    @job.input(:in).write @data
    @job.input(:n).write 2
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq @data.permutation(2).to_a
  end

  it 'can calculate repeated permutations' do
    @job.input(:ordered).write true
    @job.input(:repeat).write true
    @job.input(:in).write @data
    @job.input(:n).write 2
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq @data.repeated_permutation(2).to_a
  end
end
