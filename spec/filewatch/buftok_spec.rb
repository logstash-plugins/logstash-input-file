# encoding: utf-8
require_relative 'spec_helper'

describe FileWatch::BufferedTokenizer do

  context "when using the default delimiter" do
    it "splits the lines correctly" do
      expect(subject.extract("hello\nworld\n")).to eq ["hello", "world"]
    end

    it "holds partial lines back until a token is found" do
      buffer = described_class.new
      expect(buffer.extract("hello\nwor")).to eq ["hello"]
      expect(buffer.extract("ld\n")).to eq ["world"]
    end
  end

  context "when passing a custom delimiter" do
    subject { FileWatch::BufferedTokenizer.new("\r\n") }

    it "splits the lines correctly" do
      expect(subject.extract("hello\r\nworld\r\n")).to eq ["hello", "world"]
    end
  end
end
