"use client"

import * as React from "react"
import { cn } from "@/lib/utils"

interface SlotProps extends React.HTMLAttributes<HTMLElement> {
  asChild?: boolean
}

const Slot = React.forwardRef<HTMLElement, SlotProps>(
  ({ asChild = false, className, ...props }, ref) => {
    const Comp = asChild ? React.Fragment : "div"
    
    if (asChild) {
      return <React.Fragment {...props} />
    }
    
    return (
      <div
        ref={ref as React.Ref<HTMLDivElement>}
        className={cn(className)}
        {...props}
      />
    )
  }
)
Slot.displayName = "Slot"

export { Slot }
