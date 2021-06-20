package Controller


import Service.ServiceWordCount



class ControllerWordCount extends TController {
  private val serviceWordCount = new ServiceWordCount
//  private val daoWordCount = new DaoWordCount


  override def execute() ={
    serviceWordCount.wordCount()
  }
}
