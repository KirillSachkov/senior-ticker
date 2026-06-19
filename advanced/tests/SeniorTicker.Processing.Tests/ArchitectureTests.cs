using System.Reflection;
using SeniorTicker.Application;
using SeniorTicker.Domain;
using Xunit;

namespace SeniorTicker.Processing.Tests;

/// <summary>
/// Превращает правило зависимостей Clean Architecture в CI-гейт, а не в соглашение.
/// Domain не зависит ни от какой другой сборки решения и ни от какого стороннего пакета;
/// Application зависит только от Domain среди сборок решения.
/// </summary>
public class ArchitectureTests
{
    [Fact]
    public void Domain_references_no_solution_or_thirdparty_assemblies()
    {
        var domain = typeof(Tick).Assembly;
        var refs = domain.GetReferencedAssemblies()
            .Select(a => a.Name!)
            .ToArray();

        // никаких сборок решения
        Assert.DoesNotContain(refs, n => n.StartsWith("SeniorTicker.", StringComparison.Ordinal));
        // только BCL (System.*, netstandard, mscorlib) — ни одного стороннего NuGet
        Assert.All(refs, n => Assert.True(
            n.StartsWith("System", StringComparison.Ordinal) || n is "netstandard" or "mscorlib",
            $"Domain ссылается на неожиданную сборку: {n}"));
    }

    [Fact]
    public void Application_references_only_Domain_among_solution_assemblies()
    {
        var app = typeof(IDeduplicator).Assembly;
        var solutionRefs = app.GetReferencedAssemblies()
            .Select(a => a.Name!)
            .Where(n => n.StartsWith("SeniorTicker.", StringComparison.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { "SeniorTicker.Domain" }, solutionRefs);
    }
}
