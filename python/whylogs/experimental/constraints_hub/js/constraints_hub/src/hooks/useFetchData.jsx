import axios from "axios"
import { useEffect, useState } from "react"

export function useFetchData(url, params = {}) {
  const fetchData = async () => {
    const response = await axios.get(url, { params })
    return response?.data ?? {}
  }
  const [columnTypesDict, setColumnTypesDict] = useState({})

  useEffect(() => {
    (async () => {
      const response = await fetchData()
      setColumnTypesDict(response)
    })()
  }, [])

  return [columnTypesDict]

}
