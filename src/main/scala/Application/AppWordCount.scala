package Application


import Controller.ControllerWordCount


object AppWordCount extends App with TApp {
  start(){
    new ControllerWordCount().execute()
  }
}
