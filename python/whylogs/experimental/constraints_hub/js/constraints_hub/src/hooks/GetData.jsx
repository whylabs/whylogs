import axios from "axios"
import { useState, useEffect } from "react"

export function getData(url) {
  const [data, setData] = useState(null)

  useEffect(() => {
    axios.get(url).then((response) => {
      setData(response.data)
    })
  }, [])

  return data
}
