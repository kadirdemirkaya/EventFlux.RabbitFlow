using System.Collections.Concurrent;
using System.Reflection;

namespace EventFlux.RabbitFlow.Tests.Fakes
{
    public class RecordingProxy : DispatchProxy
    {
        public ConcurrentQueue<Invocation> Calls { get; } = new();

        public Func<MethodInfo, object?[], object?>? Responder { get; set; }

        public static (T Instance, RecordingProxy Recorder) Create<T>(Func<MethodInfo, object?[], object?>? responder = null) where T : class
        {
            var instance = Create<T, RecordingProxy>();
            var recorder = (RecordingProxy)(object)instance;
            recorder.Responder = responder;
            return (instance, recorder);
        }

        public IReadOnlyList<Invocation> CallsTo(string methodName)
            => Calls.Where(c => c.Method.Name == methodName).ToList();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            args ??= Array.Empty<object?>();

            Calls.Enqueue(new Invocation(targetMethod, args));

            var response = Responder?.Invoke(targetMethod, args);
            return response ?? DefaultResult(targetMethod.ReturnType);
        }

        private static object? DefaultResult(Type returnType)
        {
            if (returnType == typeof(void)) return null;
            if (returnType == typeof(Task)) return Task.CompletedTask;
            if (returnType == typeof(ValueTask)) return default(ValueTask);
            if (returnType == typeof(bool)) return true;

            if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                var resultType = returnType.GetGenericArguments()[0];
                return typeof(Task).GetMethod(nameof(Task.FromResult))!
                    .MakeGenericMethod(resultType)
                    .Invoke(null, new[] { DefaultValue(resultType) });
            }

            return DefaultValue(returnType);
        }

        private static object? DefaultValue(Type type)
            => type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    public record Invocation(MethodInfo Method, object?[] Args)
    {
        public T Arg<T>(string parameterName)
        {
            var index = Array.FindIndex(Method.GetParameters(), p => p.Name == parameterName);
            return (T)Args[index]!;
        }
    }
}
