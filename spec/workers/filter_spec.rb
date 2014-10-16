require 'spec_helper'

describe Workers::Filter do
  before do
    @job = SideJob.queue('core', 'Workers::Filter')
  end
  
  it 'pass through filter with a variety of data types' do
    data = [{'abc' => 123, 'xyz' => 'foo'}, [1, 2, 3, "abc"], 123, 'string', true, false, nil]
    data.each {|x| @job.input(:in).write x}
    expect(@job.input(:in).size).to eq data.length
    @job.input(:filter).write '.'
    @job.run_inline
    data.each do |x|
      expect(@job.output(:out).read).to eq x
    end
    expect(@job.status).to eq 'completed'
  end

  it 'lookup filter number' do
    @job.input(:in).write({"foo" => 42, "bar" => "hello"})
    @job.input(:filter).write '.foo'
    @job.run_inline
    expect(@job.output(:out).read).to eq 42
  end

  it 'lookup filter string' do
    @job.input(:in).write({"foo" => 42, "bar" => "hello"})
    @job.input(:filter).write '.bar'
    @job.run_inline
    expect(@job.output(:out).read).to eq "hello"
  end

  it 'string interpolation' do
    @job.input(:in).write({"foo" => 42, "bar" => "hello"})
    @job.input(:filter).write'"\(.bar) world: \(.foo+1)"'
    @job.run_inline
    expect(@job.output(:out).read).to eq 'hello world: 43'
  end

  it 'length calculation' do
    @job.input(:in).write({"foo" => [1, 2, 3]})
    @job.input(:filter).write '.foo | length'
    @job.run_inline
    expect(@job.output(:out).read).to eq 3
  end

  it 'raises error on invalid filter' do
    @job.input(:in).write({"foo" => [1, 2, 3]})
    @job.input(:filter).write 'syntax error'
    expect { @job.run_inline }.to raise_error
  end
end
