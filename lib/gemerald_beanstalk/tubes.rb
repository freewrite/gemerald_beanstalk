class GemeraldBeanstalk::Tubes

  def initialize
    @tubes = { 'default' => [] }
  end


  def tube(tube_name)
    return @tubes[tube_name] ||= GemeraldBeanstalk::Tube.new
  end




  def tubes

  end


  def tube_exists?(tube_name)
  end
end
