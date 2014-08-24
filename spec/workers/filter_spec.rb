require 'spec_helper'

describe Workers::Filter do
  it 'pass through filter' do
    job = SideJob.queue('core', 'Workers::Filter')
    data = [{'abc' => 123, 'xyz' => 'foo'}, [1, 2, 3, "abc"]]
    data.each do |x|
      job.input(:in).push_json x
    end
    job.input(:filter).push '.'
    Workers::Filter.drain
    data.each do |x|
      expect(job.output(:out).pop_json).to eq x
    end
  end

  it 'lookup filter number' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).push '.foo'
    Workers::Filter.drain
    expect(job.output(:out).pop).to eq '42'
  end

  it 'lookup filter string' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).push '.bar'
    Workers::Filter.drain
    expect(job.output(:out).pop).to eq '"hello"'
  end

  it 'string interpolation' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).push '"\(.bar) world: \(.foo+1)"'
    Workers::Filter.drain
    expect(job.output(:out).pop).to eq '"hello world: 43"'
  end

  it 'length calculation' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).push_json({"foo" => [1, 2, 3]})
    job.input(:filter).push '.foo | length'
    Workers::Filter.drain
    expect(job.output(:out).pop).to eq '3'
  end
end
