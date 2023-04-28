import axios from "axios"
import { useEffect, useState } from "react"

export function useFetchData(url) {
  const fetchData = async () => {
    const response = await axios.get(url)
    return response?.data ?? {}
  }
  const [columnTypesDict, setColumnTypesDict] = useState({})

  useEffect(() => {
    ;(async () => {
      const response = await fetchData()
      setColumnTypesDict(response)
    })()
  }, [])

  return [columnTypesDict]

  // const [data, setData] = useState(null)

  // useEffect(() => {
  //   axios.get(url)
  //     .then(response => {
  //       setData(response.data)
  //     })
  // }, [])

  return { fetchData }
}
