import axios from "axios"
import { useEffect, useState } from "react"


export function useFetchData(url) {
  
  const fetchData = async () => {
    const response = await axios.get(url);
    return response?.data ?? {}
  }
  
  // const [data, setData] = useState(null)



  // useEffect(() => {
  //   axios.get(url)
  //     .then(response => {
  //       setData(response.data)
  //     })
  // }, [])

  return { fetchData }
}
