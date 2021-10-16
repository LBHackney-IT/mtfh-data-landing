using System;

namespace MTFHDataLanding.Infrastructure.Exceptions
{
    public class EntityNotFoundException : Exception
    {
        public string EntityName { get; }
        public Guid Id { get; }

        public EntityNotFoundException(string entityName, Guid id)
            : base($"{entityName} with id {id} not found.")
        {
            EntityName = entityName;
            Id = id;
        }
    }
}