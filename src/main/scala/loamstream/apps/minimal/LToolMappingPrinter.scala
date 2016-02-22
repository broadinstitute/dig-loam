package loamstream.apps.minimal

import loamstream.map.LToolMapper.ToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LToolMappingPrinter {
  def printStore(store: LStore): String = {
    store match {
      case MiniMockStore(_, comment) => comment
      case _ => store.toString
    }
  }

  def printTool(tool: LTool): String = {
    tool match {
      case MiniMockTool(_, comment) => comment
      case _ => tool.toString
    }
  }

  def printMapping(mapping: ToolMapping): Unit = {
    for ((pile, store) <- mapping.stores) {
      println(pile + " -> " + printStore(store))
    }
    for ((recipe, tool) <- mapping.tools) {
      println(recipe + " -> " + printTool(tool))
    }
  }
}
