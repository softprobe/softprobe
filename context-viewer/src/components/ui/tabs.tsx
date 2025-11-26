import * as React from 'react';
import { cn } from '@/lib/utils';
import { X } from 'lucide-react';

export interface TabItem {
  id: string;
  label: string;
  content: React.ReactNode;
  onClose?: () => void;
  closable?: boolean;
}

interface TabsProps {
  items: TabItem[];
  activeId: string | null;
  onTabChange: (id: string) => void;
  className?: string;
}

export const Tabs: React.FC<TabsProps> = ({
  items,
  activeId,
  onTabChange,
  className,
}) => {
  const handleClose = (e: React.MouseEvent, item: TabItem) => {
    e.stopPropagation();
    if (item.onClose) {
      item.onClose();
    }
  };

  return (
    <div className={cn('flex flex-col h-full', className)}>
      {/* Tab Headers */}
      <div className="flex border-b border-gray-200 overflow-x-auto">
        {items.map((item) => {
          const isActive = item.id === activeId;
          return (
            <button
              key={item.id}
              onClick={() => onTabChange(item.id)}
              className={cn(
                'flex items-center gap-2 px-4 py-2 text-sm font-medium transition-colors',
                'border-b-2 min-w-0 flex-shrink-0',
                isActive
                  ? 'border-purple-500 text-purple-700 bg-purple-50'
                  : 'border-transparent text-gray-600 hover:text-gray-900 hover:border-gray-300',
                'whitespace-nowrap'
              )}
            >
              <span className="truncate max-w-[200px]">{item.label}</span>
              {item.closable && (
                <X
                  className="h-4 w-4 flex-shrink-0 hover:text-red-500"
                  onClick={(e) => handleClose(e, item)}
                />
              )}
            </button>
          );
        })}
      </div>

      {/* Tab Content */}
      <div className="flex-1 overflow-hidden">
        {items.map((item) => (
          <div
            key={item.id}
            className={cn(
              'h-full',
              item.id === activeId ? 'block' : 'hidden'
            )}
          >
            {item.content}
          </div>
        ))}
      </div>
    </div>
  );
};

