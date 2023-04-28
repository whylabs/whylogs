import {
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalFooter,
  ModalBody,
  ModalCloseButton,
  Button,
  FormControl,
  FormLabel,
  Input,
  Box,
  Select,
} from "@chakra-ui/react"
import { useState, useEffect } from "react"
import { useFetchData } from "../hooks/useFetchData"

const ModalComp = ({ data, setData, dataEdit, isOpen, onClose }) => {
  const [columnEntitySchema] = useFetchData(
    "http://localhost:8000/entity_schema"
  )
  const columnOptions = columnEntitySchema?.entity_schema ?? []

  const [columnTypesDict] = useFetchData(
    "http://localhost:8000/types_to_constraints"
  )
  const columnTypesToConstraints = new Map(
    Object.entries(columnTypesDict?.constraints_per_datatype ?? {})
  )

  const [column, setColumn] = useState(dataEdit.column || undefined)
  const [constraint, setConstraint] = useState(dataEdit.constraint || "")
  const [cons_value, setConsValue] = useState(dataEdit.cons_value || {})

  const renderConstraints = () => {
    if (!column || !columnTypesToConstraints) return null
    const constraints = columnTypesToConstraints.get(column.data_type)
    return constraints?.map((option) => (
      <option value={option}>{option}</option>
    ))
  }

  const handleConsValue = (target) => {
    setConsValue({ ...cons_value, [target.name]: target.value })
  }

  // TODO this API call started to bring empty arrays on my last commit,
  // didn't have time to debug it before demo-time, sorry :(

  // const [constraintParameters] = useFetchData(
  //   "http://localhost:8000/get_params",
  //   {"my_string": constraint}
  // )

  // const constraintsToValues = new Map([
  //   [constraint, [constraintParameters?.parameters]]
  // ])

  const constraintsToValues = new Map([
    ["no_missing_value", []],
    ["is_in_range", ["lower", "upper"]],
  ])

  const renderConstraintValues = () => {
    if (!constraint) return null
    const constraint_value = constraintsToValues.get(constraint)

    return constraint_value?.map((input) => (
      <FormLabel>
        {input}
        <Input
          type="text"
          name={input}
          value={cons_value[input]}
          onChange={(e) => handleConsValue(e.target)}
        />
      </FormLabel>
    ))
  }

  const handleSave = () => {
    if (!column || !constraint || !cons_value) return null

    if (Object.keys(dataEdit).length) {
      data[dataEdit.index] = { column, constraint, cons_value }
    }

    const newDataArray = !Object.keys(dataEdit).length
      ? [...(data ? data : []), { column, constraint, cons_value }]
      : [...(data ? data : [])]

    localStorage.setItem("constraint_set", JSON.stringify(newDataArray))

    setData(newDataArray)

    onClose()
  }

  return (
    <>
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Add a constraint</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <FormControl display="flex" flexDir="column" gap={4}>
              <Box>
                <FormLabel>Column</FormLabel>
                <Select
                  placeholder="Select a column"
                  value={column?.column_name}
                  onChange={(e) =>
                    setColumn(
                      columnOptions.find(
                        (option) => option.column_name == e.target.value
                      )
                    )
                  }
                >
                  {columnOptions?.map((option) => (
                    <option value={option.column_name}>
                      {option.column_name}
                    </option>
                  ))}
                </Select>
              </Box>
              <Box>
                <FormLabel>Constraint</FormLabel>
                <Select
                  placeholder="Select a constraint"
                  value={constraint}
                  onChange={(e) => setConstraint(e.target.value)}
                >
                  {renderConstraints()}
                </Select>
              </Box>
              <Box>
                <FormLabel>Values</FormLabel>
                {renderConstraintValues()}
              </Box>
            </FormControl>
          </ModalBody>

          <ModalFooter justifyContent="start">
            <Button colorScheme="green" mr={3} onClick={handleSave}>
              SAVE
            </Button>
            <Button colorScheme="gray" onClick={onClose}>
              CANCEL
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </>
  )
}

export default ModalComp
