require 'spec_helper'

describe Workers::Product do
  before do
    @job = SideJob.queue('core', 'Workers::Product')
  end

  it 'calculates array product' do
    a = [1,2,3]
    b = [true,false]
    c = [5,6,7]
    @job.input(:in).write [a, b, c]
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq a.product(b, c)
  end
end
